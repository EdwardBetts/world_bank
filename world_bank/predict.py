#!/usr/bin/env python

"""
Usage: predict.py [-h, -d, --help, --debug]

This script does something
"""

import datetime
import getopt
import logging
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
import sys
from pprint import pprint
from ConfigParser import SafeConfigParser
from sklearn import decomposition, cluster, preprocessing


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def get_data():

    # build empty entities
    years = {}
    indicators = {}

    # build dict of countries
    # todo get rid of hardcoded ref table name
    cur.execute("""
        SELECT *
        FROM ref_country
        """)
    countries = cur.fetchall()
    country_data = {}
    for country in countries:
        country_data[country[2]] = {}
#        country_data[country[2]] = {'code': country[1]}
#        country_data[country[2]]['indicators'] = {}

    # get list of all tables
    # todo get rid of hardcoded data table name
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name ilike 'master_2010_topic_9%'
        """)
    tables = cur.fetchall()
    tables = [list(i) for i in tables]

    # move through each table and get all country data
    # todo iterate over each year
    for table in tables:

        # get the year we're working on
        year = int(table[0].split('_')[1])

        # check for odd looking tables
        # todo check for issues with tables
        #av_cols = np.mean(years.values())
        #sd_cols = np.std(years.values())
        #for y in years:
        #    if years[y] < av_cols - sd_cols:
        #        print("Year has significantly fewer columns: %i, %i" % (y, years[y]))

        # get all column names in this table
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" + table[0] + "'")
        columns = cur.fetchall()
        columns = [list(i) for i in columns]

        # get all rows in this table
        cur.execute("SELECT * FROM " + table[0])
        rows = cur.fetchall()
        rows = [list(i) for i in rows]
        for row in rows:
            # todo handle the case when the num cols doesn't match num vals
            if len(columns) - len(row) != 0:
                print "ARGH, too short :("
            row = [missing_val if v is None else v for v in row]
            cdata = zip(columns, row)
            for a in cdata[1:]:
                cname = str(row[0])
                cname = ''.join(e for e in cname if e.isalnum())
                indicator = str(a[0][0])
                country_data[cname][indicator] = float(a[1])

    # return the final dict of all country data
    return country_data


def get_pca(in_data):

    # make some empty lists and counters
    good_data = []
    good_countries = []
    incompl = []
    idx_incompl = []
    bcnt = 0
    cnt = 0

    # loop through all data in dict
    for idx, val in enumerate(in_data):

        # get values for each country and make floats
        cnt += 1
        vals = in_data[val].values()
        feature_names = in_data[val].keys()
        vals = [float(n) for n in vals]

        # calculate number of missing values
        missing = float(vals.count(missing_val))
        total = float(len(vals))
        mratio = missing/total

        # handle countries with too much missing data
        if mratio > (1.0 - min_compl):
            incompl.append(val+' '+"{:.1f}%".format(100.0*(1-mratio)))
            idx_incompl.append(idx)
            bcnt += 1
            continue
        else:
            good_data.append(vals)
            good_countries.append(val)

    # give a summary of filtering
    print('%i of %i countries below min data completion (%i%%) and are excluded' % (bcnt, cnt, 100.*min_compl))
    incompl.sort()
    pprint(incompl)
    x_raw = np.asarray(good_data)

    # todo transform to polar coordinates since k-means assumes spherical symmetry
    # some code like np.polar function on x_raw

    # impute for missing values
    imp = preprocessing.Imputer(missing_values=missing_val, strategy='mean', axis=0, verbose=1)
    x_imp = imp.fit_transform(x_raw)

    # standardization of data
    x_scl = preprocessing.StandardScaler().fit_transform(x_imp)

    # print('Calculating covariance matrix')
    # cov_mat = np.cov(x_scl.T)
    # print('Decomposing covariance matrix into eigens, this may take a while...')
    # eig_vals, eig_vecs = np.linalg.eig(cov_mat)

    # decompose into singular vectors
    # u, s, v = np.linalg.svd(x_scl.T)

    print "Original dataset shape:\n%i samples\n%i features\n" % (x_scl.shape[0], x_scl.shape[1])
    pca = decomposition.PCA(n_components=4, whiten=True)
    x_pca = pca.fit(x_scl).transform(x_scl)
    print "Reduced dataset shape:\n%i samples\n%i features\n" % (x_pca.shape[0], x_pca.shape[1])

    # Percentage of variance explained for each components
    print "Variance ratio (first 2 components):\n%s\n" % str(pca.explained_variance_ratio_)

    # describe the new subspace in terms of features
    print "Meaning of the 2 components:"
    for component in pca.components_:
        print " + ".join("(%.3f x %s)" % (value, name) for value, name in zip(component, feature_names))


    # read out countries of interest
    # a = []
    # for idx, val in enumerate(x_pca):
    #     if val[0] < 0.2 and val[1] < -1.0:
    #         a.append([good_countries[idx], val])
    # a.sort(key=lambda x: x[1][1])
    # pprint(a)
    #

    return x_pca


def get_cluster(in_pca):

    # find any k-means clusters in data
    k_means = cluster.KMeans(n_clusters=4, n_jobs=-1)
    k_means.fit(in_pca)
    y_pred = k_means.predict(in_pca)
    target_names = ['low', 'medium', 'high', 'very high']

    # plot the result
    for c, i, target_name in zip("bgcmr", [0, 1, 2, 3, 4], target_names):
        plt.scatter(in_pca[y_pred == i, 0], in_pca[y_pred == i, 1], c=c, label=target_name)
    plt.plot(k_means.cluster_centers_[:, 0], k_means.cluster_centers_[:, 1], 'r*', label='centers', ms=18)
    plt.legend()
    plt.show()


def main(argv=None):
    """
    Runs the main program
    :param argv: command line arguments
    :raise RuntimeError: if something goes wrong with API wrapper
    """

    if argv is None:
        argv = sys.argv

    # read command line options
    try:
        try:
            opts, args = getopt.getopt(argv, "dh", ["debug", "help"])
        except getopt.GetoptError, err:
            raise Usage(err)
        for o, a in opts:
            if o in ("-h", "--help"):
                raise Usage(__doc__)
            elif o in ("-d", "--debug"):
                global _debug
                _debug = 1
            else:
                assert False, "unhandled option"
    except Usage, err:
        print >> sys.stderr, err.msg
        print >> sys.stderr, "for help use --help"
        return 2

    # open log file
    time0 = datetime.datetime.utcnow()

    # get some records
    data = get_data()
    pca = get_pca(data)
    get_cluster(pca)

    # report performance
    logging.info('Total runtime %s' % str(datetime.datetime.utcnow() - time0))


if __name__ == "__main__":

    # build globals
    global sql_engine
    global cur
    global missing_val
    global min_compl

    # get database config settings
    parser = SafeConfigParser()
    parser.read('world_bank.cfg')
    db_type = parser.get('database', 'type')
    db_usr = parser.get('database', 'username')
    db_pswd = parser.get('database', 'password')
    db_url = parser.get('database', 'url')
    db_name = parser.get('database', 'name')
    db_uuid = str(db_type + '://' + db_usr + ':' + db_pswd + '@' + db_url + '/' + db_name)

    # can only handle PSQL right now
    if not db_type == 'postgresql':
        raise ValueError

    # set values for global vars
    min_compl = parser.getfloat('globals', 'min_compl')
    missing_val = parser.getfloat('globals', 'missing_values')

    # open a log
    log_filename = 'log.predict'
    logging.basicConfig(filename=log_filename, level=logging.DEBUG)

    # start the SQL engine via sqlalchemy
    try:
        sql_engine = psycopg2.connect(database=db_name, user=db_usr, password=db_pswd, host=db_url)
        cur = sql_engine.cursor()
        logging.info("Connected to %s" % db_uuid)
    except:
        logging.error("Cannot connect to %s" % db_uuid)
        raise RuntimeError

    # run the program
    sys.exit(main(sys.argv[1:]))
