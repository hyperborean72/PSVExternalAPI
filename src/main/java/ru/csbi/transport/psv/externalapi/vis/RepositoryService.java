package ru.csbi.transport.psv.externalapi.vis;

import java.rmi.ServerException;
import java.util.Date;
import java.util.List;

import ru.csbi.transport.domain.core.FCOMutableEntity;
import ru.csbi.transport.domain.disp.*;
import ru.csbi.transport.domain.nsi.*;
import ru.csbi.transport.domain.xchng.ParkMapping;


public interface RepositoryService
{
  public Route findRoute(String routeNumber, TransportType type);
  public TransportType getTransportType(String transportType);
  public Transport findTransport(String parkNumber, TransportType _type, Contragent contragent);
  public Contragent getPark(String parkNumber);

  public SpecialityHistory getDriverSpecialityHistory(int driverTabnumber, Contragent contragent);

  public Clerk persistClerk(Clerk newDriver);
  public Clerk updateClerk(Clerk newDriver);

  public List<Clerk> getAllDrivers();

  public ParkMapping processParkMapping(ParkMapping mapping);

  public List<WaybillShift> getWaybillShiftsForShift(long routeId, int orderNumber, Date dateWaybill);

  public Waybill lineOn(Waybill waybill) throws WaybillLineActionException;
  public Waybill updateWaybillLineDates(Waybill waybill, Date dateBegin, Date dateEnd);

  public void saveWaybillShift(long waybillId, WaybillShift waybillShift) throws ServerException;

  public Date setLineOnDate(Date waybillDate, Date lineOnTime, Date lineOffTime);
  public Date setLineOffDate(Date waybillDate, Date lineOnTime, Date lineOffTime);

  public JobPosition getJobPosition(String jobCode);

  public List<String> getParkExternalIds();

  public void addEntity(FCOMutableEntity rw);

  public Poi getReserveWaybillStation(String parkId);

  public String checkForAddRW(ReserveWaybill bean) throws ServerException;

  public Clerk processDriver(Clerk entity);
  /*
  public Waybill updateWaybillTransport(Waybill waybill, Transport _transport);
  public WaybillShift updateWaybillShiftDriver(WaybillShift waybillShift, SpecialityHistory specialHistory);
  public Clerk getDriver(int driverTabnumber, Contragent contragent);
  public WaybillShift[] getWaybillShiftForShift(long routeId, int orderNumber, int shift, Date dateWaybill);
  */
}