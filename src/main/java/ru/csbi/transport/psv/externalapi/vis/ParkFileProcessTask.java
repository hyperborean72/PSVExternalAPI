package ru.csbi.transport.psv.externalapi.vis;

import org.apache.log4j.Logger;
import ru.csbi.transport.domain.disp.*;
import ru.csbi.transport.domain.nsi.*;

import java.io.*;
import java.rmi.ServerException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class ParkFileProcessTask implements Runnable
{
  final String CRLF = "\r\n";
  final String logFileName = "Log.csv";

  final int COLUMN_PARK_MSK = 0;
  final int COLUMN_DATE_WAYBILL_MSK = 1;
  final int COLUMN_ROUTE_MSK = 3;
  final int COLUMN_SCHEDULE_ORDER_MSK = 4;
  final int COLUMN_DATE_LINEON_MSK = 5;
  final int COLUMN_DATE_LINEOFF_MSK = 6;
  final int COLUMN_TRANSPORT_TYPE_MSK = 7;
  final int COLUMN_TRANSPORT_MSK = 8;
  final int COLUMN_SHIFT_MSK = 10;
  final int COLUMN_DRIVER_TABNUMBER_MSK = 11;
  final int COLUMN_RTYPE_MSK = 12;

  final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
  final SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");

  private static final Logger log = Logger.getLogger(ParkFileProcessTask.class);

  String planFileName;

  RepositoryService repositoryService;

  FileWriter fileWriter, shiftsFileWriter;

  String folderLocalPath;

  String folderName;

  String errorFolderPath;

  boolean errorFileOpened, shiftsFileOpened;

  String transportTypeName;

  public ParkFileProcessTask(String _planFileName,
                                 RepositoryService _repositoryService,
                                 String _folderName,
                                 String _folderLocalPath,
                                 String _errorFolderPath)
  {
    repositoryService = _repositoryService;
    planFileName = _planFileName;
    folderName = _folderName;
    folderLocalPath = _folderLocalPath;
    errorFolderPath = _errorFolderPath;
  }

  private String routeTypes;
  String[] routeTypesArray;

  public void setRouteTypes(String _routeTypes) {
    routeTypes = _routeTypes;
    routeTypesArray = Pattern.compile("[,]").split(routeTypes);
  }

  Map<String, TransportType> transportTypes = new HashMap<String, TransportType>();
  void setTransportTypes(Map<String, TransportType> _transportTypes)
  {
    transportTypes.put("Автобус", _transportTypes.get("Автобус"));

    transportTypes.put("Троллейбус", _transportTypes.get("Троллейбус"));

    transportTypes.put("Трамвай", _transportTypes.get("Трамвай"));
  }


  @Override
  public void run()
  {
    String str;
    BufferedReader reader;

    String[] fields;

    String parkNumber;
    String vehicleNumber;
    String vehicleNumberPrev = new String();
    String routeNumber;
    String routeNumberPrev = new String();
    String typeTransportId;
    String orderNumber;

    TransportType transportType;
    Contragent contragent;
    Transport transport;
    Transport transportPrev = null;

    Date dateFile;    //  дата в имени файла
    Date dateWaybill; //  дата действия путевого листа

    String timeBegin; //  время выхода
    String timeEnd;   //  время окончания
    Date dateBegin;   //  дата выхода водителя из парка
    Date dateEnd;     //  дата возвращения водителя в парк

    Integer shiftNumber;
    Integer driverTabnumber;
    SpecialityHistory driverSpecialHistory;
    Route route;
    Route routePrev = null;
    Integer order;

    Waybill waybill;
    Waybill parentWaybill;

    WaybillShift waybillShift;

    WaybillShift prevShiftWaybillShift;
    WaybillShift nextShiftWaybillShift;
    WaybillShift sameShiftWaybillShift;

    List<WaybillShift> waybillShifts;
    WaybillShift[] adjacentShifts = new WaybillShift[3];

    //Map<String, Number> routeOrderPrev = new HashMap();

    String shiftMsg;
    Poi parkPoi;

    String timePart = planFileName.substring(8,20);
    String fileName = folderLocalPath + planFileName;
    String parkCode = planFileName.substring(15, 20);

    String error;

    boolean validRouteType;


    log.info("Парсим файл выходов : " + planFileName);

    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    contragent = repositoryService.getPark(parkCode);

    if (contragent == null) {
      error = parkCode +" - код парка в имени файла соответствует несуществующему парку";
      if (!errorFileOpened)
        openErrorLog(timePart, "plan" + logFileName);
      writeErrorLog(error);
      return;
    }
    parkPoi = repositoryService.getReserveWaybillStation(parkCode);

    log.debug(parkCode + "; " + contragent + "; " + parkPoi);


    /*routeOrderPrev.put("route", 0);
    routeOrderPrev.put("order", 0);*/

    try {
      File shiftsLogFile = new File(errorFolderPath + folderName + timePart + "_shiftsLog.csv");
      shiftsLogFile.createNewFile();
      shiftsFileWriter = new FileWriter(shiftsLogFile.getAbsolutePath(), true);
    } catch (IOException e) {
      log.debug("Ошибка перечня выходов в несколько смен: " + e);
    }

    try {
      Stack<String> lines = new Stack<String>();
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "utf-8"), 4096);

      int totalLineNumber = 0;

        /* Записываем файл в стек */
      while ((str = reader.readLine()) != null) {
        str = str.trim();

        totalLineNumber++;

        /* пропускаем заголовок */
        if (totalLineNumber == 1)
          continue;

        if (str.length() == 0)
          continue;
        lines.push(str);
      }

      int lineNumber = 0;

      int directLineNumber;

      while (!lines.empty()) {

        validRouteType = false;

        directLineNumber = totalLineNumber - lineNumber;
        lineNumber++;

        str = lines.pop();
        fields = Pattern.compile("[;]").split(str, -1);

        parkNumber = fields[COLUMN_PARK_MSK];

        if (!parkNumber.equals(parkCode))
        {
          contragent = repositoryService.getPark(parkNumber);

          if (contragent == null) {
            error = contragent + ". Строка " + directLineNumber + ". Код парка соответствует несуществующему парку";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          parkPoi = repositoryService.getReserveWaybillStation(parkNumber);
        }


        if ((fields[COLUMN_RTYPE_MSK] == null) || fields[COLUMN_RTYPE_MSK].isEmpty()) {
          error = contragent + ". Строка " + directLineNumber + ". Тип маршрута не указан";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        for (String tempRouteType : routeTypesArray)
          if (fields[COLUMN_RTYPE_MSK].equals(tempRouteType)) {
            validRouteType = true;
            break;
          }

        if (!validRouteType) {
          error = contragent + ". Строка " + directLineNumber + ". Недопустимый тип маршрута";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        vehicleNumber = fields[COLUMN_TRANSPORT_MSK].replaceFirst("^0+(?!$)", "");

        if (vehicleNumber == null || vehicleNumber.isEmpty()) {
          error = contragent + ". Строка " + directLineNumber + ". Гаражный номер не указан";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        if (vehicleNumber.equals("0")) {
          error = contragent + ". Строка " + directLineNumber + ". 0 - невалидный гаражный номер";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }


        typeTransportId = fields[COLUMN_TRANSPORT_TYPE_MSK];
        if (typeTransportId.isEmpty() || typeTransportId == null) {
          error = contragent + ". Строка " + directLineNumber + ". Вид ТС не указан";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }


        switch (Integer.parseInt(typeTransportId)) {
          case 1:
            transportTypeName = "Автобус";
            break;
          case 2:
            transportTypeName = "Троллейбус";
            break;
          case 3:
            transportTypeName = "Трамвай";
            break;
          default: {
            error = contragent + ". Строка " + directLineNumber + ". Несуществующий вид транспортного средства";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }
        }

        transportType = transportTypes.get(transportTypeName);

        if (!vehicleNumber.equals(vehicleNumberPrev)){
          transport = repositoryService.findTransport(vehicleNumber, transportType, contragent);

          if (transport == null) {
            error = contragent + ". Строка " + directLineNumber + ". Не найден " + transportType + " с гаражным номером " + vehicleNumber + " в " + contragent;
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }
          log.debug(contragent + ". Строка " + directLineNumber + ", определено ТС: " + transport);

        } else {
          transport = transportPrev;
          log.debug(contragent + ". Строка " + directLineNumber + ", ТС совпадает с предыдущим: " + transport);
        }

        vehicleNumberPrev = vehicleNumber;
        transportPrev = transport;

        if (fields[COLUMN_DATE_WAYBILL_MSK].length() != 8) {
          error = contragent + ". Строка " + directLineNumber + ". Дата выхода в неверном формате";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        try {
          dateWaybill = dateFormat.parse(fields[COLUMN_DATE_WAYBILL_MSK]);

        } catch (ParseException e) {
          error = contragent + ". Строка " + directLineNumber + ". Ошибка чтения даты выхода.";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        try {
          dateFile = dateFormat.parse(planFileName.substring(0, 8));

        } catch (ParseException e) {
          error = "Имя файла выхода " + planFileName + " в неверном формате.";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        if (dateWaybill.before(dateFile)) {
          error = contragent + ". Строка " + directLineNumber + ". Дата выхода в данных не соответствует дате в имени файла. В данных: " + dateWaybill + ", в имени файла: " + dateFile;
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }


        orderNumber = fields[COLUMN_SCHEDULE_ORDER_MSK];

        if (fields[COLUMN_DATE_LINEON_MSK].length() != 6) {
          error = contragent + ". Строка " + directLineNumber + ". Время начала смены в ошибочном формате";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        if (fields[COLUMN_DATE_LINEOFF_MSK].length() != 6) {
          error = contragent + ". Строка " + directLineNumber + ". Время окончания смены в ошибочном формате";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        /* для надежности еще и это */
        if (fields[COLUMN_DATE_LINEON_MSK].isEmpty() || fields[COLUMN_DATE_LINEON_MSK] == null) {
          error = contragent + ". Строка " + directLineNumber + ". Время начала смены не указано";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        if (fields[COLUMN_DATE_LINEOFF_MSK].isEmpty() || fields[COLUMN_DATE_LINEOFF_MSK] == null) {
          error = contragent + ". Строка " + directLineNumber + ". Время окончания смены не указано";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }

        timeBegin = fields[COLUMN_DATE_LINEON_MSK];
        timeEnd = fields[COLUMN_DATE_LINEOFF_MSK];



        try {
          /* полная дата начала смены */
          dateBegin = repositoryService.setLineOnDate(dateWaybill, timeFormat.parse(timeBegin), timeFormat.parse(timeEnd));
          /* полная дата окончания смены */
          dateEnd = repositoryService.setLineOffDate(dateWaybill, timeFormat.parse(timeBegin), timeFormat.parse(timeEnd));
        } catch (ParseException e) {
          error = contragent + ". Строка " + directLineNumber + ". Время выхода {" + timeBegin + "}, возвращения {" + timeEnd + "} в неверном формате.";
          if (!errorFileOpened)
            openErrorLog(timePart, "plan" + logFileName);
          writeErrorLog(error);
          continue;
        }


          /* ФОРМИРОВАНИЕ ПУТЕВОГО ЛИСТА ВЫХОДА-СМЕН ЛИБО ПУТЕВОГО ЛИСТА РЕЗЕРВА В ЗАВИСИМОСТИ ОТ ТИПА МАРШРУТА:
             ДЛЯ НЕРЕЗЕРВНЫХ МАРШРУТОВ*/

        if (!"R".equals(fields[COLUMN_RTYPE_MSK])) {
          /*** Водитель, маршрут и смена значимы лишь для обычного п.л. ***/
          if (fields[COLUMN_DRIVER_TABNUMBER_MSK].length() > 10) {
            error = contragent + ". Строка " + directLineNumber + ". Длина табельного номера водителя превышает 10 симв.";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          routeNumber = fields[COLUMN_ROUTE_MSK];

          if (!routeNumber.equals(routeNumberPrev)) {
            route = repositoryService.findRoute(routeNumber, transportType);

            if (route == null) {
              error = contragent + ". Строка " + directLineNumber + ". Маршрут " + routeNumber + " не найден";
              if (!errorFileOpened)
                openErrorLog(timePart, "plan" + logFileName);
              writeErrorLog(error);
              continue;
            }
            log.debug(contragent + ". Строка " + directLineNumber + ", определен маршрут: " + route);

          } else {
            route = routePrev;
            log.debug(contragent + ". Строка " + directLineNumber + ", маршрут совпадает с предыдущим: " + route);
          }

          routeNumberPrev = routeNumber;
          routePrev = route;


          if (!Pattern.matches("\\d{1,}", orderNumber)) {
            error = contragent + ". Строка " + directLineNumber + ". Номер выхода не валиден";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          if (orderNumber.length() > 2)
            orderNumber = orderNumber.substring(fields[COLUMN_SCHEDULE_ORDER_MSK].length() - 2);

          order = Integer.parseInt(orderNumber);

          try {
            driverTabnumber = Integer.parseInt(fields[COLUMN_DRIVER_TABNUMBER_MSK]);
          } catch (NumberFormatException e) {
            error = contragent + ". Строка " + directLineNumber + ". Табельный номер водителя не целочисленный";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          driverSpecialHistory = repositoryService.getDriverSpecialityHistory(driverTabnumber, contragent);
          if (driverSpecialHistory == null) {
            error = contragent + ". Строка " + directLineNumber + ". Не найдена действующая запись в истории занятости для водителя с табельным номером " + driverTabnumber + " в парке " + contragent;
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          } else {
            log.debug(contragent + ". Строка " + directLineNumber + ", запись в истории занятости: " +
                    driverSpecialHistory.getId() + " " + driverSpecialHistory.getClerk().getTabNumber() + " " + driverSpecialHistory.getDateBegin());
          }

          if (fields[COLUMN_SHIFT_MSK].isEmpty()) {
            error = contragent + ". Строка " + directLineNumber + ". Смена водителя не задана для обычного маршрута";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          try {
            shiftNumber = Integer.parseInt(fields[COLUMN_SHIFT_MSK]);
          } catch (NumberFormatException e) {
            error = contragent + ". Строка " + directLineNumber + ". Номер смены не целочисленный";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          if (shiftNumber > 10) {
            error = contragent + ". Строка " + directLineNumber + ". Номер смены превышает 10";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          if (shiftNumber < 0) {
            error = contragent + ". Строка " + directLineNumber + ". Смена водителя = " + shiftNumber;
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          if (shiftNumber == 0 && !Pattern.matches("R\\d{1,}", orderNumber)) {
            error = contragent + ". Строка " + directLineNumber + ". Смена водителя = 0 для нерезервного маршрута";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          /**************************/

          /*if (!routeOrderPrev.get("order").equals(order))
            routeOrderPrev.put("order", order);

          if (!routeOrderPrev.get("route").equals(route.getId()))
            routeOrderPrev.put("route", route.getId());*/


          /* К сожалению, оптимизировать количество запросов к базе здесь нельзя:
          * количество родственных смен на том же выходе-маршруте меняется от строки к строке */
          log.debug(contragent + ". Строка " + directLineNumber + ", параметры поиска waybillshifts: " + route.getId() + " " + order);
          waybillShifts = repositoryService.getWaybillShiftsForShift(route.getId(), order, dateWaybill);
          //log.debug(contragent + ". Строка " + directLineNumber + ", по данному выходу найдено " + waybillShifts.size() + " смены." );


          if (waybillShifts != null)
            for (WaybillShift waybillShiftForShift : waybillShifts)
            {
              if (waybillShiftForShift.getShift() == shiftNumber)
                adjacentShifts[0] = waybillShiftForShift;

              if (waybillShiftForShift.getShift() == (shiftNumber+1))
                adjacentShifts[2] = waybillShiftForShift;

              if (waybillShiftForShift.getShift() == (shiftNumber-1))
                adjacentShifts[1] = waybillShiftForShift;
            }


          sameShiftWaybillShift = adjacentShifts[0];

          if (sameShiftWaybillShift != null) {
            error = contragent + ". Строка " + directLineNumber + ". Смена " + shiftNumber + " по данному выходу сформирована ранее";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          // предыдущая и последующая смены ищутся на том же путевом листе
          prevShiftWaybillShift = adjacentShifts[1];
          nextShiftWaybillShift = adjacentShifts[2];


          // ...если смена не была сформирована прежде
          waybillShift = new WaybillShift();
          waybillShift.setShift(shiftNumber);
          waybillShift.setSpecialityHistory(driverSpecialHistory);
          waybillShift.setDateBegin(dateBegin);
          waybillShift.setDateEnd(dateEnd);

          //parentWaybill - путевой лист для соседней смены на том же ТС
          parentWaybill = null;


          if (prevShiftWaybillShift != null && vehicleNumber.equals(prevShiftWaybillShift.getWaybill().getTransport().getParkNumber())) {
            if (prevShiftWaybillShift.getDateEnd().after(dateBegin)) {
              error = contragent + ". Строка " + directLineNumber + ". Предыдущая смена по данному выходу имеет время окончания позже начала текущей";
              if (!errorFileOpened)
                openErrorLog(timePart, "plan" + logFileName);
              writeErrorLog(error);
              continue;
            }

            parentWaybill = prevShiftWaybillShift.getWaybill();
            error = contragent + ". Строка " + directLineNumber + ". Родительский путевой лист с id " + parentWaybill.getId();
            log.debug(error);
          }

          if (nextShiftWaybillShift != null && vehicleNumber.equals(nextShiftWaybillShift.getWaybill().getTransport().getParkNumber())) {
            if (nextShiftWaybillShift.getDateBegin().before(dateEnd)) {
              error = contragent + ". Строка " + directLineNumber + ". Следующая смена по данному выходу имеет время начала ранее окончания текущей";
              if (!errorFileOpened)
                openErrorLog(timePart, "plan" + logFileName);
              writeErrorLog(error);
              continue;
            }

            parentWaybill = nextShiftWaybillShift.getWaybill();
            error = contragent + ". Строка " + directLineNumber + ". Родительский путевой лист с id " + parentWaybill.getId();
            log.debug(error);
          }


          if (parentWaybill == null) {
            error = contragent + ". Строка " + directLineNumber + ". Родительский путевой лист не найден. Формируем новый";
            log.debug(error);

            waybill = new Waybill(new Date(), dateWaybill, route, order, transport, dateBegin, dateEnd, WaybillSource.FILE);

             /* Запись П.Л.*/
            try {
              parentWaybill = repositoryService.lineOn(waybill);

            } catch (WaybillLineActionException we) {
              error = contragent + ". Строка " + directLineNumber + ". " + we.getMessage();
              if (!errorFileOpened)
                openErrorLog(timePart, "plan" + logFileName);
              writeErrorLog(error);
              continue;
            }
              /* привязка смены/WaybillShift к п.л. */
            try {
              repositoryService.saveWaybillShift(parentWaybill.getId(), waybillShift);
            } catch (ServerException e) {
              error = contragent + ". Строка " + directLineNumber + ". " + e.getMessage();
              if (!errorFileOpened)
                openErrorLog(timePart, "plan" + logFileName);
              writeErrorLog(error);
            }

          } else // parent waybill найден
          {
            try {
              /*
              привязка смены/WaybillShift к п.л., сохранение
              Среди прочего здесь вычисляются пересечения смен по существовавшему parentWaybill
              */
              repositoryService.saveWaybillShift(parentWaybill.getId(), waybillShift);

              shiftMsg = "Парк: " +  contragent.getName() +
                      ", маршрут: " + waybillShift.getWaybill().getRoute().getRouteNumber() +
                      ", выход: " + order +
                      ", смена: " + shiftNumber +
                      ", водитель: " + waybillShift.getDriverName();
              /*try {

                shiftsFileWriter.append(shiftMsg + CRLF);

              } catch (IOException e) {
                log.debug("Ошибка записи в перечень выходов в несколько смен " + e);
              }*/
              if (!shiftsFileOpened)
                openShiftsLog(timePart, "shifts" + logFileName);
              writeShiftsLog(shiftMsg);


            } catch (ServerException e) {
              error = contragent + ". Строка " + directLineNumber + ". " + e.getMessage();
              if (!errorFileOpened)
                openErrorLog(timePart, "plan" + logFileName);
              writeErrorLog(error);
              continue;
            }

            if (parentWaybill.getDateLineOn().after(dateBegin))
              parentWaybill = repositoryService.updateWaybillLineDates(parentWaybill, dateBegin, null);

            if (parentWaybill.getDateLineOff().before(dateEnd))
              parentWaybill = repositoryService.updateWaybillLineDates(parentWaybill, null, dateEnd);
          }
          log.debug(contragent + ". Сформирована рабочая смена " + waybillShift + " выход " + order + " путевого листа с Id=" + parentWaybill.getId());

          Arrays.fill(adjacentShifts, null);
        } else {

          /* ПУТЕВЫЕ ЛИСТЫ РЕЗЕРВА ДЛЯ РЕЗЕРВНОГО МАРШРУТА */

          ReserveWaybill rw = new ReserveWaybill();
          Date parkDep, parkArr;
          rw.setDateCreation(new Date());
          rw.setWaybill(null);
          rw.setSource(WaybillSource.FILE);
          rw.setUserInfo(null);
          rw.setDateWaybill(dateWaybill);

          try {
          /* дата выхода из парка */
            parkDep = repositoryService.setLineOnDate(dateWaybill, timeFormat.parse(timeBegin), timeFormat.parse(timeEnd));

          /* дата возвращения в парк */
            parkArr = repositoryService.setLineOffDate(dateWaybill, timeFormat.parse(timeBegin), timeFormat.parse(timeEnd));

          } catch (ParseException e) {
            error = contragent + ". Строка " + directLineNumber + ". Время выхода {" + timeBegin + "}, возвращения {" + timeEnd + "} в неверном формате.";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          order = 0;

          rw.setDateParkDep(parkDep);
          rw.setDateParkArr(parkArr);
          rw.setReserveOrderNumber(order);

          rw.setReserveOrder(null);

          if (parkPoi != null)
            rw.setStation(parkPoi);
          else
          {
            error = contragent + ". Строка " + directLineNumber + ". Не определено местоположение резервной станции парка";
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }
          rw.setTransport(transport);

          try {
            repositoryService.checkForAddRW(rw);

          } catch (ServerException e) {
            error = contragent + ". Строка " + directLineNumber + ". " + e.getMessage();
            if (!errorFileOpened)
              openErrorLog(timePart, "plan" + logFileName);
            writeErrorLog(error);
            continue;
          }

          repositoryService.addEntity(rw);
        }

      }

      reader.close();

    } catch (IOException ioe) {
      error = "Ошибка ввода-вывода: " + ioe.toString();
      log.error(error);
      if (!errorFileOpened)
        openErrorLog(timePart, "plan" + logFileName);
      writeErrorLog(error);
    } catch (Exception e) {
      error = "Прочая ошибка: " + e.toString();
      log.error(error);
      if (!errorFileOpened)
        openErrorLog(timePart, "plan" + logFileName);
      writeErrorLog(error);
    } finally {
      if (errorFileOpened)
        closeErrorLog();
      if (shiftsFileOpened)
        closeShiftsLog();
    }
    log.info("Парсинг файла выходов " + planFileName + " завершен");
  }

  /* timePart включает в себя и код парка*/
  private void openErrorLog(String timePart, String logFileName)
  {
    try{
      String errorFilePath = errorFolderPath + folderName + timePart +"_" + logFileName;

      File errorLogFile = new File(errorFilePath);
      errorLogFile.createNewFile();

      fileWriter = new FileWriter(errorLogFile.getAbsolutePath(), true);
      errorFileOpened = true;

    }catch(IOException e){
      e.printStackTrace();
    }
  }

  private void writeErrorLog(String data)
  {
    try{
      fileWriter.append(data + CRLF);

    }catch(IOException e){
      e.printStackTrace();
    }
  }

  private void closeErrorLog()
  {
    try{
      fileWriter.close();

    }catch(IOException e){
      e.printStackTrace();
    }finally{
      errorFileOpened = false;
    }
  }

  private void openShiftsLog(String timeParkPart, String shiftsLogFileName)
  {
    try{
      String shiftsFilePath = errorFolderPath + folderName + timeParkPart +"_" + shiftsLogFileName;

      File shiftsLogFile = new File(shiftsFilePath);
      shiftsLogFile.createNewFile();

      shiftsFileWriter = new FileWriter(shiftsLogFile.getAbsolutePath(), true);
      shiftsFileOpened = true;

    }catch(IOException e){
      e.printStackTrace();
    }
  }

  public void writeShiftsLog(String data)
  {
    try{
      shiftsFileWriter.append(data + CRLF);

    }catch(IOException e){
      e.printStackTrace();
    }
  }

  public void closeShiftsLog()
  {
    try{
      shiftsFileWriter.close();

    }catch(IOException e){
      e.printStackTrace();
    }finally{
      shiftsFileOpened = false;
    }
  }
}