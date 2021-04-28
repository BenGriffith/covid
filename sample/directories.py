import utils
import os

def initial():

    # Directory creation
    os.makedirs(utils.tmp_county_path)
    os.makedirs(utils.tmp_stock_path)
    os.makedirs(utils.tmp_indicator_path)
    os.makedirs(utils.county_path)
    os.makedirs(utils.stock_path)
    os.makedirs(utils.indicator_path)


def incremental(run_date, ga_run_date):

    # Incremental temporary directories
    os.makedirs('tmp/incremental/county/{}'.format(run_date))
    os.makedirs('tmp/incremental/county/{}'.format(ga_run_date))