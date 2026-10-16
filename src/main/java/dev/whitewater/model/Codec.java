package dev.whitewater.model;

/**
 * Bidirectional object ↔ wire-bytes serialization for StateStore keys
 * and payloads. The wire always carries raw bytes; a Codec keeps that
 * plumbing inside the SDK so application code works with its own types:
 *
 * <pre>{@code
 * StateStore<String, User> users =
 *         client.stateStore("app.users", Codecs.utf8(), Codecs.json(User.class));
 * users.put("user-1", new User("matt", "eng"));
 * User back = users.get("user-1").payload();          // decoded, never getBytes
 * }</pre>
 */
public interface Codec<T> {

    byte[] encode(T value);

    T decode(byte[] bytes);
}
