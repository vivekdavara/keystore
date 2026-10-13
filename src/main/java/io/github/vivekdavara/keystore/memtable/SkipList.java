package io.github.vivekdavara.keystore.memtable;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SplittableRandom;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.random.RandomGenerator;

/**
 * A sorted map as a skip list, for one writer and any number of concurrent readers.
 *
 * <p>{@link #put} must be serialised by the caller: at most one thread may be inside it at a time.
 * {@link #get}, {@link #size} and iteration take no lock and may run at the same time as that
 * writer. Nodes are never removed.
 *
 * <p>Why the readers are safe: the writer fills in a new node completely (key, value, its own next
 * pointers) and only then links it in, bottom level first, each link a volatile write (through a
 * {@link VarHandle} on the node's array of next pointers). A reader follows links with volatile
 * reads, so whatever node it reaches, it sees that node fully built.
 * It may see a node on level 0 before the node is linked on the levels above; it then finds the
 * node by going down, which is only slower. DESIGN.md has the full argument.
 */
public final class SkipList<K, V> implements Iterable<Map.Entry<K, V>> {
    /** Levels 0..11: enough for about 4^12 (16 million) keys at the expected cost. */
    static final int MAX_HEIGHT = 12;

    /** Each node is on each further level with probability 1/BRANCHING. */
    private static final int BRANCHING = 4;

    /** Volatile access to the elements of a node's next array (plain arrays have no volatile elements). */
    private static final VarHandle NEXT = MethodHandles.arrayElementVarHandle(Node[].class);

    private final Comparator<? super K> order;
    private final RandomGenerator random; // used by the writer only
    private final Node<K, V> head = new Node<>(null, null, MAX_HEIGHT);

    /** Levels in use, 1..MAX_HEIGHT. Raised by the writer before it links a taller node. */
    private volatile int height = 1;

    private volatile int size;

    public SkipList(Comparator<? super K> order) {
        this(order, new SplittableRandom());
    }

    /** With a seeded random generator, the list's shape is the same on every run (for tests). */
    SkipList(Comparator<? super K> order, RandomGenerator random) {
        this.order = Objects.requireNonNull(order, "order");
        this.random = Objects.requireNonNull(random, "random");
    }

    /** The value for {@code key}, or null if there is none. Takes no lock. */
    public V get(K key) {
        Objects.requireNonNull(key, "key");
        Node<K, V> node = findGreaterOrEqual(key, null);
        return node != null && order.compare(node.key, key) == 0 ? node.value : null;
    }

    /**
     * Sets {@code key}'s value and returns the previous value, or null if the key is new. At most one
     * thread may call this at a time.
     */
    public V put(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        @SuppressWarnings("unchecked")
        Node<K, V>[] prev = (Node<K, V>[]) new Node<?, ?>[MAX_HEIGHT];
        Node<K, V> found = findGreaterOrEqual(key, prev);
        if (found != null && order.compare(found.key, key) == 0) {
            V previous = found.value;
            found.value = value; // one volatile write: a reader sees the old value or the new one
            return previous;
        }

        int nodeHeight = randomHeight();
        int listHeight = height;
        if (nodeHeight > listHeight) {
            for (int level = listHeight; level < nodeHeight; level++) {
                prev[level] = head;
            }
            // A reader that sees the new height before the links below finds null at the head's new
            // levels, which it reads as "past the end", and goes down.
            height = nodeHeight;
        }

        Node<K, V> node = new Node<>(key, value, nodeHeight);
        // 1. Fill in the node. Nobody else can see it yet, so plain writes are enough.
        for (int level = 0; level < nodeHeight; level++) {
            node.next[level] = prev[level].next(level);
        }
        // 2. Link it in, bottom up. Each volatile write publishes the node, and everything written
        //    to it above, to any reader that follows that link.
        for (int level = 0; level < nodeHeight; level++) {
            prev[level].link(level, node);
        }
        size = size + 1; // only the writer writes size
        return null;
    }

    /** The number of keys. Takes no lock. */
    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    /** Every entry in key order. Weakly consistent while a writer runs; never throws for it. */
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return new Cursor<>(head.next(0));
    }

    /** The entries with keys at or after {@code from}, in key order. Weakly consistent. */
    public Iterator<Map.Entry<K, V>> iterator(K from) {
        Objects.requireNonNull(from, "from");
        return new Cursor<>(findGreaterOrEqual(from, null));
    }

    /**
     * The first node whose key is at or after {@code key}, or null. With {@code prev}, also records
     * the last node before {@code key} on each level in use (only the writer passes it).
     */
    private Node<K, V> findGreaterOrEqual(K key, Node<K, V>[] prev) {
        Node<K, V> node = head;
        int level = height - 1;
        while (true) {
            Node<K, V> next = node.next(level);
            if (next != null && order.compare(next.key, key) < 0) {
                node = next; // still before key: move right on this level
            } else {
                if (prev != null) {
                    prev[level] = node;
                }
                if (level == 0) {
                    return next;
                }
                level--;
            }
        }
    }

    private int randomHeight() {
        int nodeHeight = 1;
        while (nodeHeight < MAX_HEIGHT && random.nextInt(BRANCHING) == 0) {
            nodeHeight++;
        }
        return nodeHeight;
    }

    /**
     * Checks the structure, for tests, when no writer is running: every level is strictly sorted,
     * every node on a level is also on the level below, level 0 holds {@link #size} nodes, and no
     * level above the height is in use. Returns how many nodes are on each level.
     */
    int[] checkInvariants() {
        int[] counts = new int[MAX_HEIGHT];
        int listHeight = height;
        for (int level = 0; level < MAX_HEIGHT; level++) {
            List<K> keys = new ArrayList<>();
            for (Node<K, V> node = head.next(level); node != null; node = node.next(level)) {
                if (!keys.isEmpty() && order.compare(keys.get(keys.size() - 1), node.key) >= 0) {
                    throw new AssertionError("level " + level + " is not strictly sorted");
                }
                if (node.next.length <= level) {
                    throw new AssertionError("a node of height " + node.next.length + " on level " + level);
                }
                keys.add(node.key);
            }
            if (level >= listHeight && !keys.isEmpty()) {
                throw new AssertionError("level " + level + " is in use above the height " + listHeight);
            }
            counts[level] = keys.size();
            if (level > 0) {
                // Every node on this level is on the level below: walk both and match.
                Node<K, V> below = head.next(level - 1);
                for (Node<K, V> node = head.next(level); node != null; node = node.next(level)) {
                    while (below != null && below != node) {
                        below = below.next(level - 1);
                    }
                    if (below == null) {
                        throw new AssertionError("a node on level " + level + " is missing from level " + (level - 1));
                    }
                }
            }
        }
        if (counts[0] != size) {
            throw new AssertionError("level 0 has " + counts[0] + " nodes but size is " + size);
        }
        return counts;
    }

    private static final class Node<K, V> {
        final K key;
        volatile V value;
        /** Next pointers, one per level. Read and linked through {@link #NEXT}; plain writes only
         *  before the node is published. */
        final Node<K, V>[] next;

        @SuppressWarnings("unchecked")
        Node(K key, V value, int height) {
            this.key = key;
            this.value = value;
            this.next = (Node<K, V>[]) new Node<?, ?>[height];
        }

        /** A volatile read of the next pointer on {@code level}. */
        @SuppressWarnings("unchecked")
        Node<K, V> next(int level) {
            return (Node<K, V>) NEXT.getVolatile(next, level);
        }

        /** A volatile write of the next pointer on {@code level}: publishes {@code node} there. */
        void link(int level, Node<K, V> node) {
            NEXT.setVolatile(next, level, node);
        }
    }

    private static final class Cursor<K, V> implements Iterator<Map.Entry<K, V>> {
        private Node<K, V> next;

        Cursor(Node<K, V> first) {
            this.next = first;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Map.Entry<K, V> next() {
            Node<K, V> node = next;
            if (node == null) {
                throw new NoSuchElementException();
            }
            next = node.next(0);
            return new SimpleImmutableEntry<>(node.key, node.value);
        }
    }
}
