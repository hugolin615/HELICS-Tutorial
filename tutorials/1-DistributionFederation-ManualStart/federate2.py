import time
import helics as h
import random
import logging
import numpy as np

helicsversion = h.helicsGetVersion()
print("Federate 2: HELICS version = {}".format(helicsversion))

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

def destroy_federate(fed):
    """
    As part of ending a HELICS co-simulation it is good housekeeping to
    formally destroy a federate. Doing so informs the rest of the
    federation that it is no longer a part of the co-simulation and they
    should proceed without it (if applicable). Generally this is done
    when the co-simulation is complete and all federates end execution
    at more or less the same wall-clock time.

    :param fed: Federate to be destroyed
    :return: (none)
    """
    
    # Adding extra time request to clear out any pending messages to avoid
    #   annoying errors in the broker log. Any message are tacitly disregarded.
    grantedtime = h.helicsFederateRequestTime(fed, h.HELICS_TIME_MAXTIME)
    status = h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    h.helicsCloseLibrary()
    logger.info("Federate finalized")


if __name__ == "__main__":
    # main()
    logger.info("Done!")
    # based on fundamental_default/Battery.py

    np.random.seed(628)

    ##########  Registering  federate and configuring from JSON################
    fed = h.helicsCreateValueFederateFromConfig("fed_config.json")
    federate_name = h.helicsFederateGetName(fed)
    logger.info(f"Created federate {federate_name}")

    sub_count = h.helicsFederateGetInputCount(fed)
    logger.debug(f"\tNumber of subscriptions: {sub_count}")
    pub_count = h.helicsFederateGetPublicationCount(fed)
    logger.debug(f"\tNumber of publications: {pub_count}")

    print(fed.publications)
    print(fed.subscriptions)

    # Diagnostics to confirm JSON config correctly added the required
    #   publications and subscriptions
    assert(sub_count == 1)
    assert(pub_count == 1)
    #subid = {}
    #for i in range(0, sub_count):
    #subid = h.helicsFederateGetInputByIndex(fed, 0)
    subid = h.helicsFederateGetSubscription(fed, 'IEEE_123_feeder_0/totalLoad') # get subscrition by name
    sub_name = h.helicsSubscriptionGetTarget(subid)
    logger.debug(f"\tRegistered subscription---> {sub_name}")

    #pubid = {}
    #for i in range(0, pub_count):
    #pubid = h.helicsFederateGetPublicationByIndex(fed, 0) # get publication by index
    pubid = h.helicsFederateGetPublication(fed, 'TransmissionSim/transmission_voltage') # get publication by name
    pub_name = h.helicsPublicationGetName(pubid)
    logger.debug(f"\tRegistered publication---> {pub_name}")

    ##############  Entering Execution Mode  ##################################
    h.helicsFederateEnterExecutingMode(fed)
    logger.info("Entered HELICS execution mode")

    hours = 1
    #hours = 24 * 7
    total_interval = int(60 * 60 * hours)
    update_interval = int(h.helicsFederateGetTimeProperty(fed, h.HELICS_PROPERTY_TIME_PERIOD))
    print(f'DEBUG: update_interval is {update_interval}')
    grantedtime = 0


    while grantedtime < total_interval:
        logger.info("********************************************")

        c = complex(132790.562, 0) * (1 + (random.random() - 0.5)/2)
        logger.info("Voltage value = {} kV".format(abs(c)/1000))
        status = h.helicsPublicationPublishComplex(pubid, c.real, c.imag)
        #status = fed.publications['TransmissionSim/transmission_voltage'].publish((c.real, c.imag)) # this method not working; I don't know the publish's interface

        requested_time = grantedtime + update_interval
        logger.debug(f"Requesting time {requested_time}")
        grantedtime = h.helicsFederateRequestTime(fed, requested_time)
        logger.debug(f"Granted time {grantedtime}")

   
    logger.info("Destroying federate")
    destroy_federate(fed)
