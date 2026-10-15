package io.github.vivekdavara.keystore.server;

import io.github.vivekdavara.keystore.server.Protocol.ErrorCode;
import java.io.IOException;

/** An {@code ERR} reply from the server: its code and its text. */
public final class ServerException extends IOException {
    private static final long serialVersionUID = 1L;

    private final ErrorCode code;
    private final String text;

    public ServerException(ErrorCode code, String text) {
        super(code + (text.isEmpty() ? "" : ": " + text));
        this.code = code;
        this.text = text;
    }

    public ErrorCode code() {
        return code;
    }

    /** What followed the code on the line, perhaps empty. */
    public String text() {
        return text;
    }
}
