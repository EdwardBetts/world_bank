#!/usr/bin/env python

"""
Usage: scrape.py -s <start_year> -e <end_year> [-h, -d, --help, --debug]

**Note that -s and -e are mandatory**

This script scrapes the World Bank database via their API and
the Python wbdata library. Data is stored in a Pandas-style
dataframe and then written to an **existing** Postgresql database.
"""

import datetime
import getopt
import logging
import math
import pandas as pd
import sqlalchemy
import sys
import wbdata
from ConfigParser import SafeConfigParser


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def create_ref_table(reflist, keys, tname):
    """
    function to build a reference table in the output
    database for the user
    :param reflist: input list of values to form index
    :param keys: list with two values [code id, name id]
    :param tname: name of the output table, appended to 'ref_'
    """
    codes = []
    names = []
    for x in reflist:
        codes.append(''.join(e for e in x[keys[0]] if e.isalnum()))
        names.append(''.join(e for e in x[keys[1]] if e.isalnum()))

    # add code and name to a ref table in DB
    df = pd.DataFrame(index=range(len(codes)), columns=['code', 'name'])
    df['code'] = codes
    df['name'] = names
    df.to_sql('ref_' + str(tname), sql_engine, if_exists='replace', index=True, index_label=None)


def split_dataframe(df_in, tname):
    """
    Function to split large dataframes into pieces that
    postgres can handle. Using set number of columns (seg_size)
    to get below byte limit per row (8Kb)
    :param df_in: input dataframe that needs to be sharded
    :param tname: name of the master table to shard into
    """
    tcnt = 1
    scol = 0
    ncols = df_in.shape[1]
    segs = math.ceil(ncols / seg_size)
    while tcnt <= segs:
        ecol = tcnt * seg_size
        df_out = df_in[df_in.columns[scol:ecol]]
        scol = ecol
        df_out.to_sql(tname + '_' + str(tcnt), sql_engine, if_exists='replace', index=True, index_label=None)
        tcnt += 1


def main(argv=None):
    """
    Runs the main program of scraping the world bank db
    :param argv: command line arguments
    :raise RuntimeError: if something goes wrong with API wrapper
    """

    if argv is None:
        argv = sys.argv

    # read command line options
    try:
        try:
            opts, args = getopt.getopt(argv, "s:e:dh", ["debug", "help"])
        except getopt.GetoptError, err:
            raise Usage(err)
        for o, a in opts:
            if o in ("-h", "--help"):
                raise Usage(__doc__)
            elif o in ("-d", "--debug"):
                global _debug
                _debug = 1
            elif o == "-s":
                global s_yr
                s_yr = int(a)
            elif o == "-e":
                global e_yr
                e_yr = int(a)
            else:
                assert False, "unhandled option"
    except Usage, err:
        print >> sys.stderr, err.msg
        print >> sys.stderr, "for help use --help"
        return 2

    # start timing the script
    time0 = datetime.datetime.utcnow()

    # get all entities in DB
    all_entities = wbdata.search_countries('', display=False)
    topics = wbdata.get_topic(display=False)
    indicators = wbdata.get_indicator(display=False)
    if None in (all_entities, topics, indicators):
        return 2

    # declare some blank lists to hold parsed data
    cnames = []
    anames = []
    countries = []
    aggregates = []

    # separate countries from aggregates
    for c in all_entities:
        if not c['incomeLevel']['value'] == 'Aggregates':
            cnames.append(c['name'])
            countries.append(c)
        else:
            anames.append(c['name'])
            aggregates.append(c)

    # dump data to local DB
    create_ref_table(countries, ['id', 'name'], 'country')
    create_ref_table(aggregates, ['id', 'name'], 'aggregate')
    create_ref_table(indicators, ['id', 'name'], 'indicator')

    # iterate over all years in range
    while s_yr <= e_yr:

        # reformat date into readable seq
        data_date = (datetime.datetime(s_yr, 1, 1))

        # maintain list of indicators being stored
        all_ind = []

        # grab benchmark timer
        time1 = datetime.datetime.utcnow()

        # iterate over all topics
        for t in topics:

            # initialize empty dataframes
            print "\n\n%i: Working on topic: %s" % (s_yr, t['value'])
            i = 0  # start a counter
            df_cref = pd.DataFrame(index=cnames)
            df_aref = pd.DataFrame(index=anames)
            df_cmerged = df_cref
            df_amerged = df_aref

            # create table name
            naming = ['master', str(s_yr), 'topic', str(t['id'])]
            tb_cname = '_'.join(naming)
            tb_cname = tb_cname.lower()
            naming = ['aggregate', str(s_yr), 'topic', str(t['id'])]
            tb_aname = '_'.join(naming)
            tb_aname = tb_aname.lower()

            # get all indicators in a topic
            indicators = wbdata.get_indicator(topic=t['id'], display=False)

            # iterate over all indicators
            for a in indicators:

                # set a pretty name for the SQL DB
                forname = a['id'].lower()
                forname = ''.join(e for e in forname if e.isalnum())
                b = {a['id']: forname}

                # update the list of all indicators
                assert isinstance(forname, unicode)
                if forname in all_ind:
                    logging.info("%i: Skipped duplicate [%s] %s" % (s_yr, forname, a['name']))
                    continue
                all_ind.append(forname)

                # save API data to a dataframe
                df_temp = wbdata.get_dataframe(b, data_date=data_date)
                if df_temp is None:
                    logging.warn("%i: No API response [%s] %s" % (s_yr, forname, a['name']))
                    continue

                # don't bother with params below completion threshold
                notnull = df_temp.count(0) / df_cref.shape[0]
                if notnull[0] < min_compl:
                    logging.warn("%i: Too little data [%s] %s" % (s_yr, forname, a['name']))
                    continue
                print "%i: Fetched [%s] %s" % (s_yr, forname, a['name'])

                # join dataframe to empty DF or add to merged DF
                if i == 0:
                    df_cmerged = df_cref.join(df_temp)
                    df_amerged = df_aref.join(df_temp)
                else:
                    df_cmerged = df_cmerged.join(df_temp)
                    df_amerged = df_amerged.join(df_temp)
                i += 1  # increment counter

            # clean the merged dataframe of NaN to None which shows as null in db
            df_cmerged = df_cmerged.where(pd.notnull(df_cmerged), None)
            df_amerged = df_amerged.where(pd.notnull(df_amerged), None)

            # check for countries dataframe too big for PSQL db
            if df_cmerged.shape[1] > seg_size:
                logging.info("%i: Country table too large, splitting %s" % (s_yr, t['value']))
                split_dataframe(df_cmerged, tb_cname)
            else:
                df_cmerged.to_sql(tb_cname, sql_engine, if_exists='replace', index=True, index_label=None)

            # check for aggregates dataframe too big for PSQL db
            if df_amerged.shape[1] > seg_size:
                logging.info("%i: Aggregate table too large, splitting %s" % (s_yr, t['value']))
                split_dataframe(df_amerged, tb_aname)
            else:
                df_amerged.to_sql(tb_aname, sql_engine, if_exists='replace', index=True, index_label=None)
            logging.info("%i: Wrote to database %s" % (s_yr, t['value']))

        # increment the year and report performance
        logging.info('Year runtime %s' % str(datetime.datetime.utcnow() - time1))
        s_yr += 1

    # report performance
    logging.info('Total runtime %s' % str(datetime.datetime.utcnow() - time0))


if __name__ == "__main__":

    # declare some global vars
    global sql_engine
    global seg_size
    global min_compl

    # get database config settings
    parser = SafeConfigParser()
    parser.read('world_bank.cfg')
    db_type = parser.get('database', 'type')
    db_usr = parser.get('database', 'username')
    db_pswd = parser.get('database', 'password')
    db_url = parser.get('database', 'url')
    db_name = parser.get('database', 'name')
    db_uuid = str(db_type+'://'+db_usr+':'+db_pswd+'@'+db_url+'/'+db_name)

    # set values for global vars
    min_compl = parser.getfloat('globals', 'min_compl')
    seg_size = parser.getfloat('globals', 'seg_size')

    # open a log file
    log_filename = 'log.scrape'
    logging.basicConfig(filename=log_filename, level=logging.DEBUG)

    # start the SQL engine via sqlalchemy
    try:
        sql_engine = sqlalchemy.create_engine(db_uuid)
        logging.info("Connected to %s" % db_uuid)
    except:
        logging.error("Cannot connect to %s" % db_uuid)
        raise RuntimeError

    # run the program
    sys.exit(main(sys.argv[1:]))
