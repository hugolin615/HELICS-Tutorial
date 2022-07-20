import time
import helics as h
import random
import logging
import numpy as np

helicsversion = h.helicsGetVersion()
print("Federate 1: HELICS version = {}".format(helicsversion))

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

def create_broker():
    initstring = "2 --name=mainbroker"
    broker = h.helicsCreateBroker("zmq", "", initstring)
    isconnected = h.helicsBrokerIsConnected(broker)

    if isconnected == 1:
        pass

    return broker

def create_federate(deltat=1.0, fedinitstring="--federates=1"):

    fedinfo = h.helicsCreateFederateInfo()

    h.helicsFederateInfoSetCoreName(fedinfo, "Combination Federate A")
    # assert status == 0

    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    # assert status == 0

    h.helicsFederateInfoSetCoreInitString(fedinfo, fedinitstring)
    # assert status == 0

    h.helicsFederateInfoSetTimeProperty(fedinfo, h.helics_property_time_delta, deltat)
    # assert status == 0

    # h.helicsFederateInfoSetLoggingLevel(fedinfo, 1)
    # assert status == 0

    fed = h.helicsCreateCombinationFederate("Combination Federate A", fedinfo)

    return fed

def destroy_federate2(fed, broker=None):
    status = h.helicsFederateFinalize(fed)

    state = h.helicsFederateGetState(fed)
    assert state == 3

    while (h.helicsBrokerIsConnected(broker)):
        time.sleep(1)

    h.helicsFederateFree(fed)

    h.helicsCloseLibrary()

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



def main():
    # broker = create_broker() # Broker already created from 1st terminal
    fed = create_federate()
    
    # Register publication
    pubid = h.helicsFederateRegisterGlobalPublication(fed, "TransmissionSim/B2Voltage", h.helics_data_type_complex, "")
    
    # Register subscription
    subid = h.helicsFederateRegisterSubscription(fed, "DistributionSim_B2_G_1/totalLoad", "")
    
    # Register endpoint
    epid = h.helicsFederateRegisterEndpoint(fed, "ep1", None)
    
    # h.helicsSubscriptionSetDefaultComplex(subid, 0, 0)
    
    # Enter execution mode
    h.helicsFederateEnterExecutingMode(fed)
    
    hours = 1
    seconds = int(60 * 60 * hours)
    grantedtime = -1
    random.seed(0)
    for t in range(0, seconds, 60 * 5):
        c = complex(132790.562, 0) * (1 + (random.random() - 0.5)/2)
        logger.info("Voltage value = {} kV".format(abs(c)/1000))
        status = h.helicsPublicationPublishComplex(pubid, c.real, c.imag)
        # status = h.helicsEndpointSendEventRaw(epid, "fixed_price", 10, t)
        while grantedtime < t:
            grantedtime = h.helicsFederateRequestTime(fed, t)
        time.sleep(1)
        rValue, iValue = h.helicsInputGetComplex(subid)
        logger.info("Python Federate grantedtime = {}".format(grantedtime))
        logger.info("Load value = {} MVA".format(complex(rValue, iValue)/1000))
        
    t = 60 * 60 * 24
    while grantedtime < t:
        grantedtime = h.helicsFederateRequestTime(fed, t)
    logger.info("Destroying federate")
    destroy_federate(fed)

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

    # Diagnostics to confirm JSON config correctly added the required
    #   publications and subscriptions
    assert(sub_count == 1)
    assert(pub_count == 1)
    #subid = {}
    #for i in range(0, sub_count):
    subid = h.helicsFederateGetInputByIndex(fed, 0)
    sub_name = h.helicsSubscriptionGetTarget(subid)
    logger.debug(f"\tRegistered subscription---> {sub_name}")

    #pubid = {}
    #for i in range(0, pub_count):
    pubid = h.helicsFederateGetPublicationByIndex(fed, 0)
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

        requested_time = grantedtime + update_interval
        logger.debug(f"Requesting time {requested_time}")
        grantedtime = h.helicsFederateRequestTime(fed, requested_time)
        logger.debug(f"Granted time {grantedtime}")


        #rValue, iValue = h.helicsInputGetComplex((subid))
        temp = h.helicsInputGetComplex((subid))
        print(f'DEBUG: what is intpu get: {temp} ')
        #logger.info("Python Federate grantedtime = {}".format(grantedtime))
        #logger.info("Load value = {} MVA".format(complex(rValue, iValue)/1000))


    logger.info("Destroying federate")
    destroy_federate(fed)
