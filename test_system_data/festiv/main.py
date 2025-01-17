import sys
import logging
import os
import pandas as pd
import time
import helics as h

from psst.model import build_model

from psst.case import read_festiv

current_directory = os.path.realpath(os.path.dirname(__file__))

filename = os.path.join(current_directory, 'Input', 'PJM_5BUS.xlsx')
timeseries = os.path.join(current_directory, 'Input', 'TIMESERIES')

logger = logging.getLogger('psst.festiv')



def create_broker():
    initstring = "2 --name=mainbroker"
    broker = h.helicsCreateBroker("zmq", "", initstring)
    isconnected = h.helicsBrokerIsConnected(broker)

    if isconnected == 1:
        pass

    return broker


def create_value_federate(deltat=1.0, fedinitstring="--federates=1"):
    logger.debug("Creating federateinfo")
    fedinfo = h.helicsFederateInfoCreate()

    logger.debug("Setting name")
    status = h.helicsFederateInfoSetFederateName(fedinfo, "MarketSim")
    assert status == 0

    logger.debug("Setting core type")
    status = h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    assert status == 0

    logger.debug("Setting init string")
    status = h.helicsFederateInfoSetCoreInitString(fedinfo, fedinitstring)
    assert status == 0

    logger.debug("Setting time delta")
    status = h.helicsFederateInfoSetTimeDelta(fedinfo, deltat)
    assert status == 0

    logger.debug("Setting logging level")
    status = h.helicsFederateInfoSetLoggingLevel(fedinfo, 1)
    assert status == 0

    logger.debug("Creating CombinationFederate")
    fed = h.helicsCreateCombinationFederate(fedinfo)

    return fed

def destroy_value_federate(fed):
    status = h.helicsFederateFinalize(fed)

    status, state = h.helicsFederateGetState(fed)
    assert state == 3

    h.helicsFederateFree(fed)

    h.helicsCloseLibrary()

def create_mapping():
    mapping = {}

    for root, _, filenames in os.walk("../gldFeeders/"):
        for filename in filenames:
            if filename.endswith(".glm") and filename.startswith("DistributionSim"):
                bus_name = filename.split("_")[1]
                if bus_name not in mapping:
                    mapping[bus_name] = []
                mapping[bus_name].append(filename.replace(".glm", ""))
    return mapping


def build_DAM_model(day, s):

    mpc = read_festiv(filename)
    mpc.gen['RAMP_10'] = mpc.gen['PMAX']

    for i in range(0, len(s.index)):
        mpc.load.loc[i] = mpc.load.loc[0]

    for b, v in pd.read_excel(filename, sheet_name='LOAD_DIST', index_col=0,).iterrows():
        mpc.load.loc[:, b] = v.values[0] * s.values

    m = build_model(mpc)

    return m


def build_RTM_model(day, load, commitment):

    mpc = read_festiv(filename)
    mpc.gen['RAMP_10'] = mpc.gen['PMAX']

    for b, v in pd.read_excel(filename, sheet_name='LOAD_DIST', index_col=0,).iterrows():
        mpc.load.loc[:, b] = v.values[0] * load

    for col in mpc.gen['GEN_STATUS'].index:
        mpc.gen_status[col] = 0

    mpc.gen_status = mpc.gen_status.drop('GenCo0', axis=1)

    slice_df = commitment
    mpc.gen_status[slice_df.columns] = slice_df.values

    m = build_model(mpc)

    return m


def get_load(day):
    d = int(day.split('-')[-1]) - 2

    df = pd.read_excel(os.path.join(timeseries, 'ACTUAL_LOAD_DAY_{day}.xlsx'.format(day=d)), index_col=0, parse_dates=True)

    return df


def find_all_topics():

    topics = set()
    for root, _, filenames in os.walk(os.path.abspath(os.path.join(current_directory, './../DummySims/'))):
        for filename in filenames:
            if filename.endswith('.fncsPlayer'):
                with open(os.path.abspath(os.path.join(root, filename))) as f:
                    data = f.read()
                for line in data.splitlines():
                    if line.startswith('#'):
                        continue
                    line = line.replace('\t', ' ')
                    t, topic, value = line.split()
                    topics.add(topic)
    topics = list(topics)

    return topics


def main(delay=None, verbose=False):
    if verbose is not False:
        logger.setLevel(logging.DEBUG)

    mapping = create_mapping()

    logger.info("Creating CombinationFederate for FESTIV")
    fed = create_value_federate()

    pubid1 = h.helicsFederateRegisterGlobalTypePublication(fed, "AGCGenDispatch/Alta", h.HELICS_DATA_TYPE_COMPLEX, "")
    pubid2 = h.helicsFederateRegisterGlobalTypePublication(fed, "AGCGenDispatch/Brighton", h.HELICS_DATA_TYPE_COMPLEX, "")
    pubid3 = h.helicsFederateRegisterGlobalTypePublication(fed, "AGCGenDispatch/ParkCity", h.HELICS_DATA_TYPE_COMPLEX, "")
    pubid4 = h.helicsFederateRegisterGlobalTypePublication(fed, "AGCGenDispatch/Solitude", h.HELICS_DATA_TYPE_COMPLEX, "")
    pubid5 = h.helicsFederateRegisterGlobalTypePublication(fed, "AGCGenDispatch/Sundance", h.HELICS_DATA_TYPE_COMPLEX, "")

    logger.info("Registering endpoint")
    epid = h.helicsFederateRegisterGlobalEndpoint(fed, "festiv-fixed-price", "")
    if delay is not None:
        fedfilter = h.helicsFederateRegisterSourceFilter(fed, 1, "festiv-fixed-price", "delay_filter")
        status = h.helicsFilterSet(fedfilter, "delay", int(delay))

    h.helicsFederateEnterExecutionMode(fed)

    time_granted = -1
    last_second = -1
    ticker = 0

    for day in [
            '2020-08-03',
            '2020-08-04',
            '2020-08-05',
            '2020-08-06',
            '2020-08-07',
            '2020-08-08',
            '2020-08-09',
    ]:

        logger.info("Running DAM for day={day}".format(day=day))
        df = get_load(day)
        dam_s = df.loc[day, 'LOAD'].resample('1H').mean()

        dam_m = build_DAM_model(day, dam_s)
        dam_m.solve('cbc', verbose=False)

        rtm_s = df.loc[day, 'LOAD'].resample('5T').mean()

        for interval in range(0, int(24 * 60 / 5)):
            hour = int(interval * 5 / 60)
            logger.info("Running RTM for day={day} for minute={m} (hour={hour})".format(day=day, m=interval * 5, hour=hour))
            commitment = dam_m.results.unit_commitment.loc[hour:hour, :]
            rtm_m = build_RTM_model(day, rtm_s.iloc[interval], commitment)
            rtm_m.solve('cbc', verbose=False)
            logger.debug("LMP = {lmp} \t Power Generated = {pg}".format(lmp=rtm_m.results.lmp, pg=rtm_m.results.power_generated))

            for minute in range(0, 5):

                for second in range(0, 60):
                    ticker = ticker + 1

                    if int(second % 6) == 0:
                        stop_at_time = ticker
                        while time_granted < stop_at_time:
                            status, time_granted = h.helicsFederateRequestTime(fed, stop_at_time)


                        b2, b3, b4 = rtm_m.results.lmp[['B2', 'B3', 'B4']].values[0]
                        for name in mapping["B2"]:
                            status = h.helicsEndpointSendMessageRaw(epid, "{}/fixed_price".format(name), str(b2))
                        for name in mapping["B3"]:
                            status = h.helicsEndpointSendMessageRaw(epid, "{}/fixed_price".format(name), str(b3))
                        for name in mapping["B4"]:
                            status = h.helicsEndpointSendMessageRaw(epid, "{}/fixed_price".format(name), str(b4))

                        pg = rtm_m.results.power_generated.loc[0].to_dict()

                        status = h.helicsPublicationPublishDouble(pubid1, pg["ALTA"])
                        status = h.helicsPublicationPublishDouble(pubid2, pg["BRIGHTON"])
                        status = h.helicsPublicationPublishDouble(pubid3, pg["PARKCITY"])
                        status = h.helicsPublicationPublishDouble(pubid4, pg["SOLITUDE"])
                        status = h.helicsPublicationPublishDouble(pubid5, pg["SUNDANCE"])

                        logger.info("Publishing lmp B2={}".format(b2))
                        logger.info("Publishing lmp B3={}".format(b2))
                        logger.info("Publishing lmp B4={}".format(b2))

                        logger.info("Publishing pg = {}".format(pg))
                        logger.info("Current time = {minutes} ".format(minutes=ticker/60))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true",
                        help="increase output verbosity")
    parser.add_argument("--delay", type=int,
                        help="delay fixed_price")

    args = parser.parse_args()
    delay = args.delay
    verbose = args.verbose
    main(delay=delay, verbose=verbose)
