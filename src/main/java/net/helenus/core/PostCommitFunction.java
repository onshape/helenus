package net.helenus.core;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import net.helenus.support.CheckedRunnable;

public class PostCommitFunction<T, R> implements java.util.function.Function<T, R> {
    public static final PostCommitFunction<Void, Void>  NULL_ABORT = new PostCommitFunction<Void, Void>(null, null, null, false);
    public static final PostCommitFunction<Void, Void> NULL_COMMIT = new PostCommitFunction<Void, Void>(null, null, null, true);

  private final List<CheckedRunnable> commitThunks;
  private final List<CheckedRunnable> abortThunks;
  private Consumer<? super Throwable> exceptionallyThunk;
  private boolean committed;

  PostCommitFunction(List<CheckedRunnable> postCommit, List<CheckedRunnable> abortThunks,
      Consumer<? super Throwable> exceptionallyThunk,
      boolean committed) {
    this.commitThunks = postCommit;
    this.abortThunks = abortThunks;
    this.exceptionallyThunk = exceptionallyThunk;
    this.committed = committed;
  }

  private void apply(CheckedRunnable... fns) {
      try {
          for (CheckedRunnable fn : fns) {
              fn.run();
          }
      } catch (Throwable t) {
          if (exceptionallyThunk != null) {
              exceptionallyThunk.accept(t);
          }
      }
  }

  public PostCommitFunction<T, R> andThen(CheckedRunnable... after) {
    Objects.requireNonNull(after);
    if (commitThunks == null) {
      if (committed) {
          apply(after);
      }
    } else {
        for (CheckedRunnable fn : after) {
            commitThunks.add(fn);
        }
    }
    return this;
  }

  public PostCommitFunction<T, R> orElse(CheckedRunnable... after) {
    Objects.requireNonNull(after);
    if (abortThunks == null) {
      if (!committed) {
          apply(after);
      }
    } else {
        for (CheckedRunnable fn : after) {
            abortThunks.add(fn);
        }
    }
    return this;
  }

  public PostCommitFunction<T, R> exceptionally(Consumer<? super Throwable> fn) {
      Objects.requireNonNull(fn);
      exceptionallyThunk = fn;
      return this;
  }

  @Override
  public R apply(T t) {
    return null;
  }
}
