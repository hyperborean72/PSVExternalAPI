package ru.csbi.transport.psv.externalapi.vis;

import org.apache.log4j.Logger;
import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Restrictions;
import org.hibernate.jdbc.Work;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.csbi.transport.domain.core.FCOMutableEntity;
import ru.csbi.transport.domain.core.FixedHourOperationDayFactory;
import ru.csbi.transport.domain.core.OperationDayFactory;
import ru.csbi.transport.domain.disp.*;
import ru.csbi.transport.domain.mon.EventLLT;
import ru.csbi.transport.domain.nsi.*;
import ru.csbi.transport.domain.xchng.ParkMapping;
import ru.csbi.util.TimeUtil;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import java.rmi.ServerException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class RepositoryServiceImpl implements RepositoryService
{
  private OperationDayFactory operationDayFactory = new FixedHourOperationDayFactory();

  private static final Logger log = Logger.getLogger(RepositoryServiceImpl.class);
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  @PersistenceContext
  private EntityManager entityManager;


  @Override
  @Transactional( readOnly = true )
  public Route findRoute(String routeNumber, TransportType type)
  {
    if( routeNumber == null ) return null;
    if( type == null ) return null;

    Query query = entityManager.createQuery
            ("select r from disp.Route r where lower(r.routeNumber)=:number and r.transportType=:type");

    query.setParameter("number", routeNumber.toLowerCase());
    query.setParameter("type", type);

    List<Route> result = query.getResultList();

    if( result.isEmpty() ) return null;

    return result.get(0);
  }


  @Override
  //@Transactional(rollbackFor = ServerException.class)
  public void saveWaybillShift(long waybillId, WaybillShift waybillShift) throws ServerException
  {
    Waybill waybillRef = entityManager.getReference(Waybill.class, waybillId);

    /* crossingShifts - здесь не в смысле пересечения смен, а наличия в этот момент времени отработки данного водителя на каком-либо ином ТС */
    List<WaybillShift> crossingShifts = checkDriverWorkingTime(waybillRef, waybillShift);

    if(!crossingShifts.isEmpty())
    {
      log.info("Пересечение периодов работы водителя : " + crossingShifts);
      throw new ServerException("Пересечение периодов работы водителя с: "+crossingShifts);
    }

    if(waybillShift.getId()!=null)
    {
      waybillShift.setWaybill(waybillRef);
      //waybillShift = entityManager.merge(waybillShift);

      waybillRef.addDriver(waybillShift); // если не возвращать шагом ранее, то линкуется смена без ссылки на этот п.л.
      entityManager.flush();
      return;
    }

    waybillShift.setDateCreation(new Date());
    waybillShift.setWaybill(waybillRef);
    waybillShift.setSource(WaybillShiftSource.FILE);

    entityManager.persist(waybillShift);
    waybillRef.addDriver(waybillShift);
    entityManager.flush();
  }


  public List<WaybillShift> checkDriverWorkingTime(Waybill waybillRef, WaybillShift waybillShift)
  {
    final Clerk driver = waybillShift.getDriver();
    if(driver==null) return new ArrayList<WaybillShift> ();
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(WaybillShift.class);
    criteria.createAlias("specialityHistory","specialityHistory") ;
    criteria.createAlias("specialityHistory.clerk","clerk") ;

    criteria.add(Restrictions.or(Restrictions.eq("clerk.id", driver.getId()), Restrictions.eq("waybill.id", waybillRef.getId())));


    criteria.add(Restrictions.or(Restrictions.and(Restrictions.gt("dateBegin", waybillShift.getDateBegin()), Restrictions.lt("dateBegin", waybillShift.getDateEnd())),
            Restrictions.and(Restrictions.gt("dateEnd", waybillShift.getDateBegin()), Restrictions.lt("dateEnd", waybillShift.getDateEnd()))));

    return criteria.list();

  }

  /*private int getDriverWorkForDay(WaybillShift waybillShift)
  {
    int minutes=(int) TimeUnit.MILLISECONDS.toMinutes(waybillShift.getDateEnd().getTime()-waybillShift.getDateBegin().getTime());

    Date dateStart = operationDayFactory.getDayStartTime(waybillShift.getDateBegin());
    Date dateEnd = operationDayFactory.getDayEndTime(waybillShift.getDateBegin());

    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(WaybillShift.class);
    criteria.createAlias("specialityHistory","specialityHistory") ;
    criteria.createAlias("specialityHistory.clerk","clerk") ;

    criteria.add(Restrictions.eq("clerk.id", waybillShift.getDriver().getId()));

    criteria.add(Restrictions.or(Restrictions.and(Restrictions.ge("dateBegin", dateStart), Restrictions.le("dateBegin", dateEnd)),
            Restrictions.and(Restrictions.ge("dateEnd", dateStart), Restrictions.le("dateEnd", dateEnd))));

    List<WaybillShift> list = criteria.list();

    for(WaybillShift driverWork : list)
    {
      Date begin = driverWork.getDateBegin();
      Date end = driverWork.getDateEnd();

      minutes+=TimeUnit.MILLISECONDS.toMinutes(end.getTime()-begin.getTime());
    }
    log.debug("Driver's worktime in minutes : "+minutes);
    return minutes;
  }*/


  @Override
  @Transactional( readOnly = true )
  public Transport findTransport(String parkNumber, TransportType _type, Contragent contragent)
  {
    if( parkNumber == null ) return null;

    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(Transport.class, "t");
    criteria.createAlias("t.transportModel", "tm");
    criteria.createAlias("tm.transportType", "tt");
    criteria.add(Restrictions.eq("t.parkNumber", parkNumber));
    criteria.add(Restrictions.eq("tt.id", _type.getId()));
    if( contragent != null )
    {
      contragent = entityManager.getReference(Contragent.class, contragent.getId());
      Collection<Contragent> children = contragent.getSubdivisionsRecursive();
      criteria.add(Restrictions.in("t.park", children));
    }

    List<Transport> result = criteria.list();
    if( result.isEmpty() ) return null;

    return result.get(0);

  }


  @Override
  @Transactional
  public TransportOrg getPark(String parkNumber)
  {
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(ParkMapping.class, "p");
    criteria.add(Restrictions.eq("p.externalId", parkNumber));
    List<ParkMapping> result = criteria.list();

    if( result.isEmpty() ) return null;

    return result.get(0).getPark();
  }


  @Override
  @Transactional
  public TransportType getTransportType(String transportType)
  {
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(TransportType.class);
    criteria.add(Restrictions.eq("name", transportType));
    List<TransportType> result = criteria.list();

    if( result.isEmpty() ) return null;

    return result.get(0);
  }

  @Override
  @Transactional
  public SpecialityHistory getDriverSpecialityHistory(int driverTabnumber, Contragent contragent)
  {
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(SpecialityHistory.class);
    criteria.createAlias("clerk","c");
    criteria.add(Restrictions.eq("c.tabNumber", driverTabnumber));
    criteria.add(Restrictions.eq("c.contragent", contragent));
    criteria.add(Restrictions.eq("speciality", Speciality.DRIVER));
    criteria.add(Restrictions.isNull("dateEnd"));
    List<SpecialityHistory> result = criteria.list();

    if( result.isEmpty() ) return null;

    return result.get(0);
  }

  /*@Override
  @Transactional
  public Clerk getDriver(int driverTabnumber, Contragent contragent)
  {
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(Clerk.class);
    criteria.add(Restrictions.eq("tabNumber", driverTabnumber));
    criteria.add(Restrictions.eq("contragent", contragent));
    List<Clerk> result = criteria.list();

    if( result.isEmpty() ) return null;

    return result.get(0);
  }*/


  @Override
  @Transactional
  public JobPosition getJobPosition(String name)
  {
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(JobPosition.class);
    criteria.add(Restrictions.eq("name", name));
    List<JobPosition> result = criteria.list();
    if( result.isEmpty() ) return null;
    return result.get(0);
  }


  @Override
  @Transactional
  public Clerk processDriver(Clerk newDriver)
  {
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(Clerk.class);
    criteria.add(Restrictions.eq("tabNumber", newDriver.getTabNumber()));
    List<Clerk> list = criteria.list();

    final Date currentDate = new Date();
    if( list.isEmpty() )
    {
      newDriver.setSpeciality(Speciality.DRIVER);
      entityManager.persist(newDriver);


      SpecialityHistory specialityHistory = new SpecialityHistory();
      specialityHistory.setDateBegin(currentDate);
      specialityHistory.setClerk(newDriver);
      specialityHistory.setSpeciality(Speciality.DRIVER);
      entityManager.persist(specialityHistory);
      return newDriver;
    }

    Clerk driver = list.get(0);
    Hibernate.initialize(driver.getSpecialityHistorySet());
    final Set<SpecialityHistory> specialityHistorySet = driver.getSpecialityHistorySet();
    if(specialityHistorySet.isEmpty()){
      SpecialityHistory specialityHistory = new SpecialityHistory();
      specialityHistory.setDateBegin(currentDate);
      specialityHistory.setClerk(driver);
      specialityHistory.setSpeciality(Speciality.DRIVER);
      entityManager.persist(specialityHistory);
    }
    for( SpecialityHistory specialityHistoryA : specialityHistorySet )
    {
      if(specialityHistoryA.getDateEnd()==null && specialityHistoryA.getSpeciality()!=Speciality.DRIVER){

        specialityHistoryA.setDateEnd(currentDate);
        entityManager.merge(specialityHistoryA);

        SpecialityHistory specialityHistory = new SpecialityHistory();
        specialityHistory.setDateBegin(currentDate);
        specialityHistory.setClerk(driver);
        specialityHistory.setSpeciality(Speciality.DRIVER);
        entityManager.persist(specialityHistory);
        break;
      }
    }
    if( !driver.getName().equals(newDriver.getName()) )
    {
      log.info("Обновляем водителя с табельным номером " + newDriver.getTabNumber());
      driver.setName(newDriver.getName());
      entityManager.flush();
    }
        /* если табельный не новый, возвращаем найденного водителя вне зависимости от наличия-отсутствия изменений */
    return driver;
  }


  @Override
  @Transactional
  public Clerk persistClerk(Clerk newDriver)
  {
    final Date currentDate = new Date();
    newDriver.setSpeciality(Speciality.DRIVER);
    entityManager.persist(newDriver);

    SpecialityHistory specialityHistory = new SpecialityHistory();
    specialityHistory.setDateBegin(currentDate);
    specialityHistory.setClerk(newDriver);
    specialityHistory.setSpeciality(Speciality.DRIVER);
    entityManager.persist(specialityHistory);
    return newDriver;
  }

  @Override
  @Transactional
  public Clerk updateClerk(Clerk driver)
  {
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(Clerk.class);
    criteria.add(Restrictions.eq("tabNumber", driver.getTabNumber()));
    List<Clerk> list = criteria.list();

    Clerk driver0 = list.get(0);

    final Date currentDate = new Date();
    Hibernate.initialize(driver0.getSpecialityHistorySet());
    final Set<SpecialityHistory> specialityHistorySet = driver0.getSpecialityHistorySet();
    if(specialityHistorySet.isEmpty()){
      SpecialityHistory specialityHistory = new SpecialityHistory();
      specialityHistory.setDateBegin(currentDate);
      specialityHistory.setClerk(driver0);
      specialityHistory.setSpeciality(Speciality.DRIVER);
      entityManager.persist(specialityHistory);
    }
    for( SpecialityHistory specialityHistoryA : specialityHistorySet )
    {
      if(specialityHistoryA.getDateEnd()==null && specialityHistoryA.getSpeciality()!=Speciality.DRIVER){

        specialityHistoryA.setDateEnd(currentDate);
        entityManager.merge(specialityHistoryA);

        SpecialityHistory specialityHistory = new SpecialityHistory();
        specialityHistory.setDateBegin(currentDate);
        specialityHistory.setClerk(driver0);
        specialityHistory.setSpeciality(Speciality.DRIVER);
        entityManager.persist(specialityHistory);
        break;
      }
    }
    if( !driver0.getName().equals(driver.getName()) )
    {
      log.info("Обновляем водителя с табельным номером " + driver.getTabNumber());
      driver0.setName(driver.getName());
      entityManager.flush();
      log.info("Обновление водителя с табельным номером " + driver.getTabNumber() + " завершено");
    }
        /* если табельный не новый, возвращаем найденного водителя вне зависимости от наличия-отсутствия изменений */
    return driver0;
  }



  @Override
  @Transactional
  public List<Clerk> getAllDrivers()
  {
    Query query = entityManager.createQuery
            ("select r from nsi.Clerk r");

    List<Clerk> result = query.getResultList();

    if( result.isEmpty() ) return null;

    return result;
  }

  @Override
  @Transactional
  public ParkMapping processParkMapping(ParkMapping mapping)
  {
    Session session = (Session) entityManager.getDelegate();

    /* если внутренний id парка из текущей записи встречается в таблице маппингов,
       существующую запись для него удаляем */
    Criteria criteriaPark = session.createCriteria(ParkMapping.class);
    criteriaPark.add(Restrictions.eq("park", mapping.getPark()));
    List<ParkMapping> previousMappings = criteriaPark.list();

    if(!previousMappings.isEmpty())
    {
      entityManager.remove(previousMappings.get(0));
      entityManager.flush();
    }

    //если внешний id парка из текущей записи в базе не найден, запись сохраним
    Criteria criteriaExtId = session.createCriteria(ParkMapping.class);
    criteriaExtId.add(Restrictions.eq("externalId", mapping.getExternalId()));
    List<ParkMapping> list = criteriaExtId.list();

    if(list.isEmpty())
    {
      entityManager.persist(mapping);
      entityManager.flush();
      return mapping;
    }

    // если внешний id парка из текущей записи в базе найден, но в загрузке ссылается на другой парк, запись обновим
    ParkMapping parkMapping = list.get(0);

    if (!parkMapping.getPark().equals(mapping.getPark()))
    {
      parkMapping.setPark(mapping.getPark());
      entityManager.flush();
    }
    return parkMapping;
  }


  /* все смены по данному выходу */
  @Override
  @Transactional
  public List<WaybillShift> getWaybillShiftsForShift(long routeId, int orderNumber, Date dateWaybill){

    Session session = (Session) entityManager.getDelegate();
    Criteria waybillShiftCriteria = session.createCriteria(WaybillShift.class);
    waybillShiftCriteria.createAlias("waybill", "w");

    waybillShiftCriteria
            .add(Restrictions.eq("w.dateWaybill", dateWaybill))
            .add(Restrictions.eq("w.scheduleOrderNumber", orderNumber))
            .add(Restrictions.eq("w.route.id", routeId));

    List<WaybillShift> list = waybillShiftCriteria.list();

    if(list.isEmpty())
      return null;

    return list;
  }


  /*public WaybillShift[] getWaybillShiftForShift(long routeId, int orderNumber, int shift, Date dateWaybill){

    WaybillShift[] waybillShifts = new WaybillShift[3];

    Session session = (Session) entityManager.getDelegate();
    Criteria waybillShiftCriteria = session.createCriteria(WaybillShift.class);
    waybillShiftCriteria.createAlias("waybill", "w");

    waybillShiftCriteria
            .add(Restrictions.eq("w.dateWaybill", dateWaybill))
            .add(Restrictions.eq("w.scheduleOrderNumber", orderNumber))
            .add(Restrictions.eq("w.route.id", routeId))
            .setCacheable(true);


    List<WaybillShift> waybillShiftsForShift = waybillShiftCriteria.list();

    for (WaybillShift waybillShiftForShift : waybillShiftsForShift)
    {
      if (waybillShiftForShift.getShift() == shift)
        waybillShifts[0] = waybillShiftForShift;

      if (waybillShiftForShift.getShift() == (shift+1))
        waybillShifts[2] = waybillShiftForShift;

      if (waybillShiftForShift.getShift() == (shift-1))
        waybillShifts[1] = waybillShiftForShift;
    }


    return waybillShifts;
  }*/


  // заимствовано из WaybillRepository с ИЗМЕНЕНИЯМИ
  @Override
  //@Transactional(rollbackFor = WaybillLineActionException.class)
  public Waybill lineOn(Waybill waybill) throws WaybillLineActionException
  {
    Session session = (Session)entityManager.getDelegate();
    session.setCacheMode(CacheMode.NORMAL);

    // Проверка на соответствие типа ТС - маршруту
    TransportType tt = waybill.getRoute().getTransportType();
    TransportType tt2 = waybill.getTransport().getTransportType();

    if( tt == null || tt2 == null )
      log.debug("TransportType is NULL:" + tt + " TT2=" + tt2);

    if( tt != null && tt2 != null && !tt.equals(tt2) )
    {
      session.setCacheMode(CacheMode.NORMAL);
      throw new WaybillLineActionException
              ("Ошибка выпуска на линию. Вид ТС " + tt2 + " не соответствует маршруту № " + waybill
                      .getRoute() + ", имеющий тип " + tt);
    }

    Long orderId = callGetScheduleOrder(waybill);
    ScheduleOrder order = entityManager.getReference(ScheduleOrder.class, orderId);

    waybill.setScheduleOrder(order);

    //Первоначальная инициализация времени действия путевого листа
    //без проверки, что все смены по данному наряду работают на одном ТС
    //Игнорируем установку dateLineOn, если произведена в VisImporter
    // на основании данных ПЛ предыдущей смены

    if (waybill.getDateLineOn() == null)
      //schedule_order.park_depart_time(park_arrive_time)
      //хранит время в минутах от полуночи в пользовательской временной зоне
      waybill.setDateLineOn(setLineOnDate(waybill.getDateWaybill(),order.getParkDepartTime().addMinutes(-180).getDate(), order.getParkArriveTime().addMinutes(-180).getDate()));
    if (waybill.getDateLineOff() == null)
      waybill.setDateLineOff(setLineOnDate(waybill.getDateWaybill(), order.getParkDepartTime().addMinutes(-180).getDate(), order.getParkArriveTime().addMinutes(-180).getDate()));

    List<Waybill> queryResult = findCrossingWaybills(waybill,true);
    SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm");

    if( !queryResult.isEmpty() )
    {
      Waybill earlyWaybill = queryResult.get(0);

      session.setCacheMode(CacheMode.NORMAL);
      throw new WaybillLineActionException
              ("Ошибка выпуска на линию. ТС №" + earlyWaybill.getTransport() + " уже выпущено по маршруту(" + earlyWaybill
                      .getRoute() +
                      "), выход №" + earlyWaybill.getScheduleOrderNumber() +
                      ", время выезда " + sdf.format(earlyWaybill.getDateLineOn()) + " возвращение " + sdf
                      .format(earlyWaybill.getDateLineOff()) + ", выберите другой маршрут или выход.");
    }

    List<ScheduleRouteValidity> schedulesActivity = findScheduleActivity(waybill);

    if( schedulesActivity.isEmpty() )
    {
      session.setCacheMode(CacheMode.NORMAL);
      throw new WaybillLineActionException(
              "Ошибка выпуска на линию. ТС №" + waybill.getTransport() + " отсутствуют активные расписания на " + sdf
                      .format(waybill.getDateLineOn())
                      + ", по маршруту " + waybill.getRoute() + ", выберите другой маршрут.");
    }

    //SPD-179
    Date dateLineOn =waybill.getDateLineOn();
    final Criteria eventLLTCriteria = session.createCriteria(EventLLT.class);
    eventLLTCriteria.add(Restrictions.eq("route.id", waybill.getRoute().getId()));
    eventLLTCriteria.add(Restrictions.eq("orderNumber", waybill.getScheduleOrderNumber()));
    Criterion dateEventCriterion =Restrictions.and(Restrictions.le("dateBegin", TimeUtil.clearMillis(dateLineOn)),
            Restrictions.ge("dateEnd", TimeUtil.clearMillis(dateLineOn)));
    eventLLTCriteria.add(dateEventCriterion);
    List<EventLLT> eventLLTList =eventLLTCriteria.list();
    if(!eventLLTList.isEmpty()){
      EventLLT eventLLT =eventLLTList.get(0);
      LineoffReason lineoffReason = eventLLT.getLineoffReason();
      Integer minFixTimeValue = 0;
      if(lineoffReason !=null){
        Integer minFixTime = lineoffReason.getMinFixTime();
        minFixTimeValue = minFixTime ==null?0:minFixTime;
      }

      Date eventLLTDateBegin =eventLLT.getDateBegin();
      Date eventLLTDateBeginWithMax = new Date(eventLLTDateBegin.getTime()+TimeUnit.MINUTES.toMillis(minFixTimeValue));

      if(eventLLTDateBeginWithMax.after(dateLineOn)){
        removeEventLLT(session,eventLLT);
      }else{
        eventLLT.setDateEnd(dateLineOn);
        saveEventLLT(session,eventLLT);
      }
    }

    try
    {
      entityManager.persist(waybill);
      entityManager.flush();
    }
    catch( PersistenceException e )
    {
      log.error("Error: " + e, e);

      Throwable cause = e.getCause();
      for(; cause.getCause() != null; cause = cause.getCause() )
      {
      }

      String msg = cause.getMessage();

      session.setCacheMode(CacheMode.NORMAL);
      throw new WaybillLineActionException(msg);
    }

    Hibernate.initialize(waybill.getTransport());
    Hibernate.initialize(waybill.getRoute());
    Hibernate.initialize(waybill.getScheduleOrder().getRaces());

    session.setCacheMode(CacheMode.NORMAL);

    return waybill;
  }

  @Override
  @Transactional
  public Waybill updateWaybillLineDates(Waybill waybill, Date _dateBegin, Date _dateEnd)
  {
    Waybill waybillFound = entityManager.find(Waybill.class, waybill.getId());

    if(waybillFound == null)
    {
      entityManager.persist(waybill);
      return waybill;
    }

    Hibernate.initialize(waybillFound.getWaybillShiftList());
    if (_dateBegin != null) waybillFound.setDateLineOn(_dateBegin);
    if (_dateEnd != null) waybillFound.setDateLineOff(_dateEnd);

    entityManager.flush();

    return waybillFound;
  }

  /*@Override
  @Transactional
  public Waybill updateWaybillTransport(Waybill waybill, Transport _transport)
  {
    Waybill waybillFound = entityManager.find(Waybill.class, waybill.getId());

    if(waybillFound == null)
    {
      entityManager.persist(waybill);
      return waybill;
    }

    Hibernate.initialize(waybillFound.getWaybillShiftList());
    if (_transport != null) waybillFound.setTransport(_transport);

    entityManager.flush();

    return waybillFound;
  }

  @Override
  @Transactional
  public WaybillShift updateWaybillShiftDriver(WaybillShift waybillShift, SpecialityHistory specialHistory)
  {
    WaybillShift thisWaybillShift = entityManager.find(WaybillShift.class, waybillShift.getId());

    if(thisWaybillShift == null)
    {
      entityManager.persist(waybillShift);
      return waybillShift;
    }

    thisWaybillShift.setSpecialityHistory(specialHistory);

    entityManager.flush();

    return thisWaybillShift;
  }*/



  public List<ScheduleRouteValidity> findScheduleActivity(Waybill waybill)
  {
    Session session = (Session) entityManager.getDelegate();
    Criteria criteria = session.createCriteria(ScheduleRouteValidity.class);

    ScheduleRouteValidity.addRouteRestriction(criteria, waybill.getRoute().getId());
    criteria.add(Restrictions.le("dateBegin", waybill.getDateLineOn()));
    criteria.add(Restrictions.or(Restrictions.isNull("dateEnd"), Restrictions.gt("dateEnd", waybill.getDateLineOn())));

    return criteria.list();
  }

  public List<Waybill> findCrossingWaybills(Waybill _waybill,boolean isNewWaybill)
  {
    String qlString = "select w from disp.Waybill w " +
            " where" +
            " ((w.route.id = :routeId and w.scheduleOrderNumber = :orderNumber)" +
            " or (w.transport.id = :transportId))" +
            // " and w.dateLineOff is null" +
            " and (" +
            " (w.dateLineOn > :dateOn and w.dateLineOff < :dateOff)" +
            " or (w.dateLineOn < :dateOn and w.dateLineOff >= :dateOff)" +
            " or (w.dateLineOn >= :dateOn and w.dateLineOn < :dateOff)" +
            " or (w.dateLineOff > :dateOn and w.dateLineOff <= :dateOff)" +
            " )";
    if(!isNewWaybill){
      qlString = qlString +" and w.id<>:id";
    }
    Query query = entityManager.createQuery(qlString);

    query.setParameter("routeId", _waybill.getRoute().getId());
    query.setParameter("orderNumber", _waybill.getScheduleOrderNumber());
    query.setParameter("transportId", _waybill.getTransport().getId());
    query.setParameter("dateOn", _waybill.getDateLineOn());
    query.setParameter("dateOff", _waybill.getDateLineOff());
    if(!isNewWaybill){
      query.setParameter("id", _waybill.getId());

    }
    return query.getResultList();
  }


  public void saveEventLLT(Session session, EventLLT event)
  {
    checkEventLLTDatesBeforeSave(event);
    ru.csbi.transport.domain.adm.ActionType at = event.getId() == null ? ru.csbi.transport.domain.adm.ActionType.CREATE : ru.csbi.transport.domain.adm.ActionType.EDIT;
    if( event.getId() == null )
      session.persist(event);
    else
      session.merge(event);
  }

  public void removeEventLLT(Session session, EventLLT event)
  {
    session.delete(event);
  }

  public void checkEventLLTDatesBeforeSave(EventLLT _event)
  {
    if(_event.getDateBegin().after(_event.getDateEnd())){
      Calendar c = Calendar.getInstance(UTC);
      c.setTime(_event.getDateEnd());
      c.add(Calendar.DAY_OF_MONTH, 1);
      _event.setDateEnd(c.getTime());
    }
  }

  public Long callGetScheduleOrder(Waybill newWaybill) throws WaybillLineActionException
  {
    Session session = (Session) entityManager.getDelegate();

    GetScheduleOrder getScheduleOrder = new GetScheduleOrder(newWaybill);

    session.doWork(getScheduleOrder);

    return getScheduleOrder.getResult();
  }

  public static Date calculateDateWaybillForGetOrderFunction(Date _dateWaybill,boolean isNight)
  { Calendar calendar = Calendar.getInstance();
    calendar.setTime(_dateWaybill);
    if( isNight )
    {
      calendar.add(Calendar.HOUR_OF_DAY, 1);
    }
    else
    {
      calendar.add(Calendar.HOUR_OF_DAY, -3);
    }

    //Вычисляем час, чтобы позже в при преобразовании в utc получилось 00:00:00
    Date time = new Date();
    final TimeZone defaultTimeZone = TimeZone.getDefault();
    int diffMilliSeconds =defaultTimeZone
            .getOffset(time.getTime()) - UTC.getOffset(time.getTime());
    long diffHours = TimeUnit.MILLISECONDS.toHours(diffMilliSeconds);

    calendar.set(Calendar.HOUR_OF_DAY, (int) diffHours);

    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    _dateWaybill = calendar.getTime();
    return _dateWaybill;
  }


  private static class GetScheduleOrder implements Work
  {
    private Waybill newWaybill;

    private Long result;
    private SQLException resultException;

    private Route newWaybillRoute;

    public GetScheduleOrder(Waybill _newWaybill) {newWaybill = _newWaybill;}

    @Override
    public void execute(Connection connection) throws SQLException
    {
      CallableStatement cs = connection.prepareCall
              ("{? = call DISP_WayBill_PKG.GetSchedule_Order(?, ?, ?, ?)}");

      try
      {
        cs.registerOutParameter(1, Types.BIGINT);

        Date dateWaybill = newWaybill.getDateWaybill();

        newWaybillRoute = newWaybill.getRoute();
        Date dateWaybillOperDate = calculateDateWaybillForGetOrderFunction(dateWaybill,newWaybillRoute.isNight());

        Timestamp sqlDate = new Timestamp(dateWaybillOperDate.getTime());
        Timestamp sqlDateDW = new Timestamp(dateWaybill.getTime());
        cs.setObject(2, sqlDate);
        cs.setObject(3, newWaybillRoute.getId());
        cs.setObject(4, newWaybill.getScheduleOrderNumber());
        cs.setObject(5, sqlDateDW);
        try
        {
          cs.execute();
          result = cs.getLong(1);
        }
        catch( SQLException e )
        {
          log.debug("Waybill afterPersist:", e);

          resultException = e;
        }
      }
      finally
      {
        cs.close();
      }
    }

    public Long getResult() throws WaybillLineActionException
    {
      if( resultException != null )
        throw new WaybillLineActionException("Ошибка выпуска на линию. " +
                "Не найдено действующее расписание по маршруту " + newWaybillRoute.getRouteNumber() +
                " с номером выпуска " + newWaybill.getScheduleOrderNumber());

      return result;
    }
  }


  // Формирует полную дату выпуска путевого листа из даты и времени
  @Override
  public Date setLineOnDate(Date waybillDate, Date lineOnTime, Date lineOffTime)
  {
    Calendar c = Calendar.getInstance();
    c.setTime(waybillDate);

    Calendar cTimePart = Calendar.getInstance();
    cTimePart.setTime(lineOnTime);


        /* независимо от типа маршрута, если ТС выпущено после 3 утра следующих суток, оно должно было быть заведено в следующей выгрузке,
         * поэтому такие ситуации не рассматриваем */

    if (lineOnTime.compareTo(lineOffTime)<0 && cTimePart.get(Calendar.HOUR_OF_DAY)<3)
      c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) + 1);


    c.set(Calendar.HOUR_OF_DAY, cTimePart.get(Calendar.HOUR_OF_DAY));
    c.set(Calendar.MINUTE, cTimePart.get(Calendar.MINUTE));
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);

    return c.getTime();
  }


  // Формирует полную дату снятия путевого листа из даты и времени
  @Override
  public Date setLineOffDate(Date waybillDate, Date lineOnTime, Date lineOffTime)
  {
    Calendar c = Calendar.getInstance();
    c.setTime(waybillDate);

    Calendar cTimePart = Calendar.getInstance();
    cTimePart.setTime(lineOffTime);

    Calendar cTimePartLineOn = Calendar.getInstance();
    cTimePartLineOn.setTime(lineOnTime);

    if (lineOnTime.compareTo(lineOffTime)>0)
      c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) + 1);

    if (lineOnTime.compareTo(lineOffTime)<0 && cTimePartLineOn.get(Calendar.HOUR_OF_DAY)<3)
      c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) + 1);

    c.set(Calendar.HOUR_OF_DAY, cTimePart.get(Calendar.HOUR_OF_DAY));
    c.set(Calendar.MINUTE, cTimePart.get(Calendar.MINUTE));
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);

    return c.getTime();
  }


  @Override
  @Transactional
  public List<String> getParkExternalIds()
  {
    Query query = entityManager.createQuery("select pm.externalId from xchng.ParkMapping pm");

    List<String> list = query.getResultList();

    log.info("Внешние ключи парков в базе:");
    for (String key : list)
      log.info(key);

    return list;
  }

  @Override
  @Transactional
  public void addEntity(FCOMutableEntity rw)
  {
    entityManager.persist(rw);
    entityManager.flush();
  }



  /* определение ДС | КП действующего варианта маршрута для заданного маршрута*/
  @Override
  @Transactional
  public Poi getReserveWaybillStation(String parkId) {

    return getPark(parkId).getParkPoi();
  }


  /* проверка валидности вновь созданного путевого листа резерва перед его сохранением */
  @Override
  @Transactional
  public String checkForAddRW(ReserveWaybill bean) throws ServerException
  {
    Session session = (Session)entityManager.getDelegate();

    SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm");

    String res = "";
    if (bean.getDateParkArr().before(bean.getDateParkDep()))
    {
      res = "Время выхода из парка превышает время возврата в парк";
      log.info("Время выхода из парка превышает время возврата в парк");
      throw new ServerException(res);
    }

    Date start  = bean.getDateParkDep();
    Date finish = bean.getDateParkArr();

    Criteria criteria_1 = session.createCriteria(ReserveWaybill.class)
            .add(Restrictions.eq("transport.id", bean.getTransport().getId()))
            .add(Restrictions.or(
                    Restrictions.or(
                            Restrictions.between("dateParkDep", start, finish),
                            Restrictions.between("dateParkArr", start, finish)),
                    Restrictions.and(
                            Restrictions.lt("dateParkDep", start),
                            Restrictions.gt("dateParkArr", finish) ) )
            );

    List<ReserveWaybill> list_1 = criteria_1.list();

    if( !list_1.isEmpty() )
    {
      ReserveWaybill rw0 = list_1.get(0);
      res = "Для ТС " + rw0.getTransport() + " путевой лист резерва на " + rw0.getStation().getName() +
              " на период "+sdf.format(rw0.getDateParkDep()) + " - " + sdf.format(rw0.getDateParkArr()) +
              " сформирован ранее.";
      throw new ServerException(res);
    }

    /*Criteria criteria_2 = session.createCriteria(ReserveWaybill.class)
            .add(Restrictions.eq("station.id",   bean.getStation().getId()))
            .add(Restrictions.eq("reserveOrderNumber", bean.getReserveOrderNumber()))
            .add(Restrictions.or(
                    Restrictions.or(
                            Restrictions.between("dateParkDep", start, finish),
                            Restrictions.between("dateParkArr", start, finish)),
                    Restrictions.and(
                            Restrictions.lt("dateParkDep", start),
                            Restrictions.gt("dateParkArr", finish)) )
            );

    List<ReserveWaybill> list_2 = criteria_2.list();

    if( !list_2.isEmpty() )
    {
      ReserveWaybill rw0 = list_2.get(0);
      res =    "Путевой лист резерва для выхода " + rw0.getReserveOrderNumber() +
              " на " + rw0.getStation().getName() +
              " на период "+sdf.format(rw0.getDateParkDep()) + " - " + sdf.format(rw0.getDateParkArr()) +
              " был сформирован ранее.";
      throw new ServerException(res);
    }*/
    return res;
  }
}