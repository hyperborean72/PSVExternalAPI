package ru.csbi.transport.psv.externalapi.vis;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TaskProcessingInfo
{
  private Future<?> future;

  public TaskProcessingInfo(Future<?> _future)
  {
    future = _future;
  }

  public void waitForCompletion() throws ExecutionException, InterruptedException
  {
    future.get();
  }
}
