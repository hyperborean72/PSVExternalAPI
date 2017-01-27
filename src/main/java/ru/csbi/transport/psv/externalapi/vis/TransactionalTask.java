package ru.csbi.transport.psv.externalapi.vis;

import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

public class TransactionalTask implements Runnable
{
  private TransactionTemplate transactionTemplate;
  private Runnable task;

  public TransactionalTask(TransactionTemplate _transactionTemplate, Runnable _task)
  {
    transactionTemplate = _transactionTemplate;
    task = _task;
  }

  @Override
  public void run()
  {
    transactionTemplate.execute(new TransactionCallbackWithoutResult()
    {
      @Override
      protected void doInTransactionWithoutResult(TransactionStatus status)
      {
        task.run();
      }
    });
  }
}
