package ru.csbi.transport.psv.externalapi.vis;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.io.CopyStreamException;
import org.apache.log4j.Logger;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.support.TransactionTemplate;
import ru.csbi.transport.domain.nsi.*;
import ru.csbi.transport.domain.xchng.ExternalSystem;
import ru.csbi.transport.domain.xchng.ParkMapping;
import ru.csbi.util.profiling.Stopwatch;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.io.*;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class VisImporter {

  private RepositoryService repositoryService;

  @PersistenceContext
  private EntityManager entityManager;

  final String logFileName = "Log.csv";
  public static final String CRLF = "\r\n";

  private String ftpServer;
  private String ftpUsername;
  private String ftpPassword;
  private Integer ftpPort;

  private String importPath;
  private String folderNameTemplate;

  private String folderName;
  private String folderLocalPath;
  private String errorFolderPath;

  private int daysBehind;
  String routeTypes;

  BufferedWriter bufferWriter;
  FileWriter fileWriter, shiftsFileWriter;

  List<String> filesForTheDate = new ArrayList<String>();
  List<String> remoteFileNamesLast = new ArrayList<String>();

  boolean errorFileOpened;
  boolean directoryExists;

  List<String> parkExternalIds;

  Map<String, TransportType> transportTypes = new HashMap<String, TransportType>();

  private static final Logger log = Logger.getLogger(VisImporter.class);

  private AsyncTaskExecutor taskExecutor;
  public void setTaskExecutor(AsyncTaskExecutor _taskExecutor)
  {
    taskExecutor = _taskExecutor;
  }

  private TransactionTemplate transactionTemplate;
  public void setTransactionTemplate(TransactionTemplate _transactionTemplate)
  {
    transactionTemplate = _transactionTemplate;
  }

  public void doJob() {

    final Collection<TaskProcessingInfo> taskList = new ArrayList<TaskProcessingInfo>();

    log.info("VIS Importer start");
    setLocalPathToParse();

    /* Здесь формируется remoteFileNamesLast */
    try{
      copyFromFTP();
    } catch (FtpConnectionException e)
    {
      log.error("Дальнейшее исполнение программы остановлено из-за ошибок FTP доступа ");
      return;
    }

    if(!directoryExists)
    {
      log.info("VIS Importer finish.");
      return;
    }
    log.info("remoteFileNamesLast: " +remoteFileNamesLast);

    String parkMappingFileName = remoteFileNamesLast.remove(0);
    if (!parkMappingFileName.equals(""))
      mergeParks(parkMappingFileName);
    else
      log.info("Файл парков отсутствует");

    String driverFileName = remoteFileNamesLast.remove(0);
    if (!driverFileName.equals(""))
      mergeDrivers(driverFileName);
    else
      log.info("Файл водителей отсутствует");

    List<String> lastPlanFiles = findLastPlanFiles(remoteFileNamesLast);

    /* определить Id вида транспорта в нашей системе */
    setTransportTypes();

    log.info(getTransportTypes());

    for (String planFileName : lastPlanFiles) {
      ParkFileProcessTask task = new ParkFileProcessTask(planFileName, repositoryService, folderName, folderLocalPath, errorFolderPath);
        /* аргумент сеттера инициализирован Spring*/
      task.setRouteTypes(routeTypes);
        /* аргумент сеттера инициализирован в локальном setTransportTypes() */
      task.setTransportTypes(transportTypes);

      //taskExecutor.submit(new TransactionalTask(transactionTemplate, task));
      Future<?> future = taskExecutor.submit(new TransactionalTask(transactionTemplate, task));

        /*
          Насколько я понимаю, формировать TaskProcessingInfo
          и коллекцию taskList необязательно
         */
      taskList.add(new TaskProcessingInfo(future));
    }

    // Ожидание завершения задач
    try {
      waitForCompletion(taskList);
    } catch (Exception e) {
      log.error("Поток прерван с исключением: " + e);
    }

    taskList.clear();


    List<String> processedParkIds = new ArrayList<String>();

    for (String lastPlanFile : lastPlanFiles)
      processedParkIds.add(lastPlanFile.substring(15, 20));

    log.info("processedParkIds: " + processedParkIds);

    /* перечень парков, по которым не получены плановые наряды */
    try {
      String msg;

      /* parkExternalIds установлен ранее в findFiles() */
      for (String parkExternalId : parkExternalIds) {

        if (!processedParkIds.contains(parkExternalId)) {

            File missingParksLogFile = new File(errorFolderPath + folderName + "_missingParksLog.csv");
            missingParksLogFile.createNewFile();
            fileWriter = new FileWriter(missingParksLogFile.getAbsolutePath(), true); //true = append file

            msg = "Не предоставлены данные по плановым выходам парков: ";
            fileWriter.append(msg + CRLF);

          msg = repositoryService.getPark(parkExternalId).getName();
          fileWriter.append(msg + CRLF);
        }
      }
      fileWriter.close();

    } catch (IOException e) {
      log.debug("Ошибка формирования перечня парков с отсутствующими заданиями по нарядам: "+ e);
    }

    resetInstanceVars();

    log.info("VIS Importer finish.");
  }

  /* В списке файлов удаленной директории определяем позднейшие файлы парков и водителей
   * и полный перечень файлов выходов */
  public List<String> findFiles(List<String> fileNameList) {

    filesForTheDate.clear();

    String driverFileName = new String();
    String parkFileName = new String();
    List<String> planFileNames = new ArrayList<String>();

    for (String fileName : fileNameList) {

      if (fileName.matches("^\\d{14}_driver.csv") && fileName.split("_driver")[0].compareTo(driverFileName) > 0) {
        driverFileName = fileName;
        continue;
      }

      if (fileName.matches("^\\d{14}_park.csv") && fileName.split("_park.csv")[0].compareTo(parkFileName) > 0) {
        parkFileName = fileName;
        continue;
      }

      if (fileName.matches("^\\d{14}_P.*.csv$"))
                /* в случае идентификатора парка в начале имени файла
                    if (fileName.matches("^P.*.csv$"))
                 */
        planFileNames.add(fileName);
    }

    filesForTheDate.add(parkFileName);
    filesForTheDate.add(driverFileName);

    filesForTheDate.addAll(planFileNames);

    log.info("Файлы парков и водителей для процессинга и все имена файлов выходов: " + filesForTheDate.toString());
    return filesForTheDate;
  }


  /* В передаваемом списке имен файлов выходов определяем позднейшие */
  public List<String> findLastPlanFiles(List<String> planFileNames) {

    List<String> planFiles = new ArrayList<String>();

    parkExternalIds = repositoryService.getParkExternalIds();

    for (String parkExternalId : parkExternalIds)
    {
      String thisParkFileName = new String();

      for (String planFileName : planFileNames)
      {
        if ((Pattern.matches("^\\d{14}_"+parkExternalId+".csv", planFileName) || Pattern.matches("^\\d{14}_"+parkExternalId+"_.csv", planFileName)) && planFileName.compareTo(thisParkFileName) > 0)
        {
          thisParkFileName = planFileName;
        }
      }

      if (!thisParkFileName.isEmpty())
        planFiles.add(thisParkFileName);
    }

    log.info("Отобранные последние файлы выходов для процессинга : " + planFiles.toString());
    return planFiles;
  }


  /* Переносим из папки за интересующие сутки последние версии файлов */
  public void copyFromFTP() throws FtpConnectionException{
    FTPClient client = new FTPClient();
    FileOutputStream fos = null;

    /* формируем локальную папку для хранения выгруженных данных */
    File downloadToFolder = new File(folderLocalPath);

    if (!downloadToFolder.exists()) {
      if (downloadToFolder.mkdir()) {
        log.info("Директорий " + folderLocalPath + " создан!");
      } else {
        log.error("Директорий " + folderLocalPath + " не создан!");
      }
    }

    /* формируем локальную папку для хранения лога ошибок */
    errorFolderPath = folderLocalPath + "log/";
    File errorFolder = new File(errorFolderPath);

    if (!errorFolder.exists()) {
      if (errorFolder.mkdir()) {
        log.info("Директорий " + errorFolder + " создан!");
      } else {
        log.error("Директорий " + errorFolder + " не создан!");
      }
    }

    log.info("Обращаемся к папке " + folderName + " на внешнем FTP");

    try {
      client.setDataTimeout(1000 * 60 * 10);
      client.connect(ftpServer, ftpPort);

      if (!client.login(ftpUsername, ftpPassword)) {
        log.error("Аутентификация на сервере " + ftpServer + " не удалась.");
        return;
      }

      client.enterLocalPassiveMode();

      log.info("Ответ FTP сервера при аутентификации: " + client.getReplyString());

      // On connection verify reply code
      int reply = client.getReplyCode();
      if (!FTPReply.isPositiveCompletion(reply)) {
        client.disconnect();
        log.error("FTP сервер " + ftpServer + " запретил соединение.");
        return;
      }

      String remoteFolderPath = "/" + folderName + "/";
      directoryExists = client.changeWorkingDirectory(remoteFolderPath);

      if (!directoryExists)
      {
        log.info("Директорий за интересующие транспортные сутки отсутствует на FTP сервере");
        return;
      }

      FTPFile[] ftpFiles = client.listFiles(remoteFolderPath);

      if (ftpFiles == null)
      {
        log.error("FTP server " + ftpServer + " refused connection.");
      }

      if (ftpFiles != null)
      {
        List<String> remoteFileNames = new ArrayList<String>();

        for (FTPFile file : ftpFiles)
        {
          if (!file.isFile())
          {
            continue;
          }
          remoteFileNames.add(file.getName());

        }
        /* читаем на ftp последние версии файлов по паркам и водителям,
           а также полный список файлов выходов */
        remoteFileNamesLast = findFiles(remoteFileNames);

      }

      for (String remoteFileLast : remoteFileNamesLast) {
        if (!remoteFileLast.equals("")) {
          log.info("Копируется файл " + remoteFileLast);
          try {
            fos = new FileOutputStream(folderLocalPath + remoteFileLast);
            client.retrieveFile("/" + folderName + "/" + remoteFileLast, fos);
            /* где: new FileOutputStream() */
          } catch (FileNotFoundException e) {
            log.error("Файл " + remoteFileLast + " либо не существует либо недоступен");
            /* где: client.retrieveFile() */
          } catch (CopyStreamException e) {
            log.error("Ошибка на этапе чтения файла: " + e.getIOException().getMessage());
          } catch (IOException e) {
            log.error("Ошибка ввода-вывода: " + e.getMessage());
          } finally {
            fos.close();
          }
        }
      }

        /* где: client.connect() */
    } catch (SocketException e) {
      log.error("Таймаут сокета не может быть выставлен");
      throw new FtpConnectionException();

        /* где: client.connect() */
    } catch (UnknownHostException e) {
      log.error("Неизвестный адрес сервера");
      throw new FtpConnectionException();

        /* где: client.retrieveFile() или client.login() или client.changeWorkingDirectory() или или client.listFiles() */
    } catch (FTPConnectionClosedException e) {
      log.error("Сервер преждевременно закрыл соединение");
      throw new FtpConnectionException();

    } catch (IOException e) {
      log.error("Ошибка ввода-вывода: " + e.getMessage());
    } finally {
      try {
        if (fos != null)
          fos.close();
        client.logout();
        client.disconnect();
      } catch (IOException e) {
        log.error(e, e);
      }
    }
  }



  public void mergeDrivers(String driverFileName) {

    log.info("Парсим файл водителей : " + driverFileName);
    BufferedReader reader;
    String str;
    String[] fields;
    String[] fio;

    String timePart = driverFileName.substring(8,14);
    String fileName = folderLocalPath + driverFileName;

    Contragent contragent;
    Clerk clerk;

    int tabNum;

    JobPosition driverJob = repositoryService.getJobPosition("Водитель");

    List<Clerk> clerks = repositoryService.getAllDrivers();

    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "utf-8"), 4096);
      int lineNumber = 0;

      try {
        while ((str = reader.readLine()) != null) {
          lineNumber++;
          if (lineNumber == 1)
            continue;

          str = str.trim();
          if (str.length() == 0)
            continue;

          fields = Pattern.compile("[;]").split(str);


          contragent = repositoryService.getPark(fields[0]);
          if (contragent == null)
          {
            String error = "Строка " + lineNumber + ". Код парка соответствует несуществующему парку";
            if (!errorFileOpened)
              openErrorLog(timePart, "driver" + logFileName);
            writeErrorLog(error);
            continue;
          }

          if (fields[1] == null || fields[1].isEmpty())
          {
            String error = "Строка " + lineNumber + ". Отсутствует табельный номер водителя";
            if (!errorFileOpened)
              openErrorLog(timePart, "driver" + logFileName);
            writeErrorLog(error);
            continue;
          }

          try {
            tabNum = Integer.parseInt(fields[1]);
          } catch (NumberFormatException e) {
            String error = "Строка " + lineNumber + ". Табельный номер водителя нецелочисленный";
            if (!errorFileOpened)
              openErrorLog(timePart, "driver" + logFileName);
            writeErrorLog(error);
            continue;
          }


          if (fields.length == 2)
          {
            String error = "Строка " + lineNumber + ". Отсутствует имя водителя";
            if (!errorFileOpened)
              openErrorLog(timePart, "driver" + logFileName);
            writeErrorLog(error);
            continue;
          }

          if (fields[2].length() > 128)
          {
            String error = "Строка " + lineNumber + ". Длина имени водителя превышает 128 симв.";
            if (!errorFileOpened)
              openErrorLog(timePart, "driver" + logFileName);
            writeErrorLog(error);
            continue;
          }


          String errorFio = "";
          fio = Pattern.compile("[ ]").split(fields[2]);
          for (String namePart : fio)
          {
            if (!Pattern.matches("^\\.{0,1}\\s{0,1}[Ёёа-яА-Я\\u002D]+", namePart))
            //if (!Pattern.matches("(.?)(\\s?)[Ёёа-яА-Я\\u002D]+", namePart))
            {
              errorFio = "Строка " + lineNumber + ". Имя водителя содержит небуквенные символы";
              if (!errorFileOpened)
                openErrorLog(timePart, "driver" + logFileName);
              writeErrorLog(errorFio);
            }
          }
          if (!errorFio.isEmpty())
            continue;

          clerk = new Clerk(fields[2], tabNum, contragent, "+7", driverJob);

          boolean isKnown = containsClerk(clerks, tabNum);

          if (!isKnown)
          {
            repositoryService.persistClerk(clerk);

          } else {

            repositoryService.updateClerk(clerk);
          }
        }

        if (lineNumber == 0){
          String error = "Пустой файл водителей";
          if (!errorFileOpened)
            openErrorLog(timePart, "driver" + logFileName);
          writeErrorLog(error);
        }

        reader.close();

      } catch (IOException ioe) {
        log.error("ОШИБКА ЧТЕНИЯ ФАЙЛА " + fileName, ioe);
      } finally {
        if (errorFileOpened)
          closeErrorLog();
      }

    } catch (Exception e) {
      log.error("ФАЙЛ НЕ НАЙДЕН. ", e);
    }
    log.info("Парсинг файла водителей завершен");
  }


  public void mergeParks(String parkFileName) {

    log.info("Парсим файл парков : " + parkFileName);
    BufferedReader reader;
    String str;
    String[] fields;

    String timePart = parkFileName.substring(8,14);
    String fileName = folderLocalPath + parkFileName;

    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "utf-8"), 4096);
      int lineNumber = 0;
      try {
        while ((str = reader.readLine()) != null) {
          ParkMapping mapping = new ParkMapping();
          lineNumber++;
          if (lineNumber == 1)
            continue;

          str = str.trim();
          if (str.length() == 0)
            continue;

          Query query = entityManager.createQuery("select e from xchng.ExternalSystem e where" +
                  " e.systemName='easufhd'");

          List<ExternalSystem> externalSystems = query.getResultList();
          if (externalSystems.isEmpty())
          {
            String error = "Строка " + lineNumber + ". Не найдено имя внешней системы";
            if (!errorFileOpened)
              openErrorLog(timePart, "park" + logFileName);
            writeErrorLog(error);
            continue;
          }

          fields = Pattern.compile("[;]").split(str);

          if (fields == null || fields.length != 2)
          {
            String error = "Строка " + lineNumber + ". Неполные данные по парку";
            if (!errorFileOpened)
              openErrorLog(timePart, "park" + logFileName);
            writeErrorLog(error);
            continue;
          }

          if (!fields[0].matches("P\\d{1,}"))
          {
            String error = "Строка " + lineNumber + ". Некорректный внешний ключ парка";
            if (!errorFileOpened)
              openErrorLog(timePart, "park" + logFileName);
            writeErrorLog(error);
            continue;
          }


          mapping.setExternalSystem(externalSystems.get(0));

          query = entityManager.createQuery("select p from nsi.Park p where" +
                  " UPPER(p.name)=UPPER(:parkName)");
          query.setParameter("parkName",fields[1]);

          List<TransportOrg> parks = query.getResultList();

          if (parks.isEmpty())
          {
            String error = "Строка " + lineNumber + ". Парк " + fields[1] + " не обнаружен. Заводим в систему";
            if (!errorFileOpened)
              openErrorLog(timePart, "park" + logFileName);
            writeErrorLog(error);

            TransportOrg newPark = new TransportOrg();

            newPark.setName(fields[1]);

            repositoryService.addEntity(newPark);

            mapping.setPark(newPark);
          } else {

            mapping.setPark(parks.get(0));
          }

          if (fields[0].length() > 1024)
          {
            String error = "Строка " + lineNumber + ". Длина внешнего идентификатора парка превышает 1024 симв.";
            if (!errorFileOpened)
              openErrorLog(timePart, "park" + logFileName);
            writeErrorLog(error);
            continue;
          }
          mapping.setExternalId(fields[0]);
          repositoryService.processParkMapping(mapping);
        }

        if (lineNumber == 0){
          String error = "Пустой файл парков";
          if (!errorFileOpened)
            openErrorLog(timePart, "park" + logFileName);
          writeErrorLog(error);
        }

        reader.close();

      } catch (IOException ioe) {
        log.error("ОШИБКА ЧТЕНИЯ ФАЙЛА " + fileName, ioe);
      } finally {
        if (errorFileOpened)
          closeErrorLog();
      }

    } catch (Exception e) {
      log.error("ФАЙЛ НЕ НАЙДЕН. ", e);
    }
    log.info("Парсинг файла парков завершен");
  }


  public void openErrorLog(String timePart, String logFileName)
  {
    try{
      String errorFilePath = errorFolderPath + folderName + timePart +"_" + logFileName;

      File errorLogFile = new File(errorFilePath);
      errorLogFile.createNewFile();

      fileWriter = new FileWriter(errorLogFile.getAbsolutePath(), true); //true = append file
      bufferWriter = new BufferedWriter(fileWriter);
      errorFileOpened = true;

    }catch(IOException e){
      e.printStackTrace();
    }
  }

  public void writeErrorLog(String data)
  {
    try{
      fileWriter.append(data + CRLF);

    }catch(IOException e){
      e.printStackTrace();
    }
  }

  public void closeErrorLog()
  {
    try{
      bufferWriter.close();
      fileWriter.close();

    }catch(IOException e){
      e.printStackTrace();
    }finally{
      errorFileOpened = false;
    }
  }

  public void resetInstanceVars()
  {
    filesForTheDate.clear();
    remoteFileNamesLast.clear();
  }

  public Date getImportDate(int shift) {
    Calendar c = Calendar.getInstance();
    c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) - shift);
    return c.getTime();
  }

  public void setFtpServer(String ftpServer) {
    this.ftpServer = ftpServer;
  }

  public void setFtpUsername(String ftpUsername) {
    this.ftpUsername = ftpUsername;
  }

  public void setFtpPassword(String ftpPassword) {
    this.ftpPassword = ftpPassword;
  }

  public void setFtpPort(Integer ftpPort) {
    this.ftpPort = ftpPort;
  }

  public void setImportPath(String _importPath) {
    importPath = _importPath;
  }

  public void setRepositoryService(RepositoryService _repositoryService) {
    repositoryService = _repositoryService;
  }

  public void setDaysBehind(int shift) {
    this.daysBehind = shift;
  }

  public void setFolderName(String _folderName) {
    folderName = _folderName;
  }

  public void setFolderLocalPath(String _folderLocalPath) {
    folderLocalPath = _folderLocalPath;
  }

  public void setFolderNameTemplate(String _folderNameTemplate) {
    folderNameTemplate = _folderNameTemplate;
  }

  private void setLocalPathToParse() {
    folderName = MessageFormat.format(folderNameTemplate, getImportDate(daysBehind));
    folderLocalPath = importPath + "/" + folderName + "/";
  }

  public void setRouteTypes(String _routeTypes) {
    routeTypes = _routeTypes;
  }

  public void setErrorFolderPath(String _errorFolderPath) {
    errorFolderPath = _errorFolderPath;
  }

  public void setTransportTypes()
  {
    transportTypes.put("Автобус", repositoryService.getTransportType("Автобус"));

    transportTypes.put("Троллейбус", repositoryService.getTransportType("Троллейбус"));

    transportTypes.put("Трамвай", repositoryService.getTransportType("Трамвай"));
  }

  public String getTransportTypes()
  {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<String, TransportType> entry : transportTypes.entrySet()){
      String type = entry.getKey();
      String name = entry.getValue().getName();

      sb.append(type + ", ");
      sb.append(name + "; ");
    }

    return sb.toString();
  }

  public static boolean containsClerk(Collection<Clerk> clerks, int tabNumber) {

    for(Clerk o : clerks) {

      if(o.getTabNumber() == tabNumber) {

        return true;
      }
    }
    return false;
  }

  private void waitForCompletion(Collection<TaskProcessingInfo> taskList)
          throws InterruptedException
  {
    Stopwatch stopwatch = new Stopwatch();
    long messageGap = TimeUnit.SECONDS.toMillis(10);
    long lastMessageTime = 0;

    int taskCount = taskList.size();

    String lastMessage = "Ожидание завершения исполнения пула задач. Осталось задач: " + taskCount;
    log.debug(lastMessage);

    for( TaskProcessingInfo taskInfo : taskList )
    {
      try
      {
        taskInfo.waitForCompletion();
      }
      catch( ExecutionException e)
      {
        Throwable cause = e.getCause();

        log.debug("Error while processing line " + taskInfo + ": " + cause, cause);
      }

      taskCount--;

      long elapsedTime = stopwatch.getElapsedTime();

      if( elapsedTime - lastMessageTime > messageGap )
      {
        lastMessage = "Ожидание завершения исполнения пула задач. Осталось задач: " + taskCount;
        log.debug(lastMessage);
        lastMessageTime = elapsedTime;
      }
    }
  }

}