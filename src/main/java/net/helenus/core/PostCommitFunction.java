package net.helenus.core;

import java.util.List;
import java.util.Objects;

public class PostCommitFunction<T, R> implements java.util.function.Function<T, R> {
    public static final PostCommitFunction<Void, Void>  NULL_ABORT = new PostCommitFunction<>(null, null, false);
    public static final PostCommitFunction<Void, Void> NULL_COMMIT = new PostCommitFunction<>(null, null, true);

  private final List<CommitThunk> commitThunks;
  private final List<CommitThunk> abortThunks;
  private boolean committed;

  PostCommitFunction(
      List<CommitThunk> postCommit,
      List<CommitThunk> abortThunks,
      boolean committed) {
    this.commitThunks = postCommit;
    this.abortThunks = abortThunks;
    this.committed = committed;
  }

  public PostCommitFunction<T, R> andThen(CommitThunk after) {
    Objects.requireNonNull(after);
    if (commitThunks == null) {
      if (committed) {
        after.apply();
      }
    } else {
      commitThunks.add(after);
    }
    return this;
  }

  public PostCommitFunction<T, R> orElse(CommitThunk after) {
    Objects.requireNonNull(after);
    if (abortThunks == null) {
      if (!committed) {
        after.apply();
      }
    } else {
      abortThunks.add(after);
    }
    return this;
  }

  @Override
  public R apply(T t) {
    return null;
  }
}
