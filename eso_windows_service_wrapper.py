from ServerFiles import setup_logging

logger = setup_logging("service.log", "C:\\Temp")

# Import other modules after setting up logging
import win32serviceutil
import win32service
import win32event
import servicemanager
from eso_update_schedule import main as scheduler_main
import sys
from threading import Event

logger.debug("Modules imported successfully.")


class ESOService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ESODataService"
    _svc_display_name_ = "ESO Data Update Service"
    _svc_description_ = "This service updates ESO data at regular intervals."

    def __init__(self, args):
        logger.info("Attempting to initialize...")
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        logger.info("Service initialized.")
        self.stop_event = Event()

    def SvcStop(self):
        logger.info("The ESO Data Update Service is stopping.")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.stop_event.set()

    def SvcDoRun(self):
        logger.info("The ESO Data Update Service has started.")
        try:
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, ""),
            )
            self.main()
        except Exception as e:
            logger.error("Exception during service start: %s", str(e))
            self.SvcStop()

    # def main(self):
    #     scheduler_main()  # Call the main function from eso_update_schedule.py
    #     while self.is_running:
    #         win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
    def main(self):
        try:
            scheduler_main(self.stop_event)
        except Exception as e:
            logger.error("Failed in scheduler_main: %s", str(e))
            raise

    def install(self):
        # Call the base class install to do the initial installation tasks
        service_path = (
            sys.executable
        )  # Path to the Python executable running the service
        print("Installing service with path:", service_path)
        logger.info(f"Installing the service, executable path: {service_path}")
        win32serviceutil.ServiceFramework.install(self)
        logger.info("Service installed successfully.")


if __name__ == "__main__":
    logger.info("Calling service_wrapper:")
    try:
        if len(sys.argv) == 1:
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(ESOService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            win32serviceutil.HandleCommandLine(ESOService)
    except Exception as e:
        logger.error("Failed in main loop: %s", str(e))
        raise
