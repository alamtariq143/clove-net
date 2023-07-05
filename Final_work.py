import sys
import boto3
import pandas as pd
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from functools import reduce
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'SOURCE_LOCATION', 'OUTPUT_LOCATION'])
source = args['SOURCE_LOCATION']
destination = args['OUTPUT_LOCATION']

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
    

## Functions ##################################################################

def to_snake_case(df):
    return df.toDF(*[c.lower().replace(' ', '_') for c in df.columns])

def trim_spaces(df):
    return  df.select(*[trim(col(c)).alias(c) for c in df.columns])

def format_dates(col, frmts=('yyyy-MM-dd HH:mm:ss', 'yyyy-MM-dd HH:mm:ss.SSS')):
    return coalesce(*[to_timestamp(col, frmt) for frmt in frmts])

## Setup lists for appending dataframes of each site ##########################

availability_list = []
mseg_list = []
mseg_q1_list = []
oee_list = []
opn_list = []
po_list = []
rt_ftr_list = []
ve_list = []


## Get settings per site ######################################################

site_settings = pd.read_excel(
    '{}/{}'.format(source, 'Global settings per Site.xlsx'),  
    engine='openpyxl'
)


###############################################################################
## Get manufacturing sites ####################################################
###############################################################################

client = boto3.client('s3')
bucket_name = source.split('/')[2]

# prefixes
wonderware_prefix = "pre-stage/mesdashb/mes/wonderware/"
aspentech_prefix = "pre-stage/mesdashb/mes/aspentech/"

# get list of objects
ww_result = client.list_objects(
    Bucket=bucket_name, Prefix=wonderware_prefix, Delimiter='/')
at_result = client.list_objects(
    Bucket=bucket_name, Prefix=aspentech_prefix, Delimiter='/')

# get sites from lists
wonderware_sites = [
    o.get('Prefix').split('/')[-2] for o in ww_result.get('CommonPrefixes')]
aspentech_sites = [
    o.get('Prefix').split('/')[-2] for o in at_result.get('CommonPrefixes')]


###############################################################################
## WonderWare transformations for each site ###################################
###############################################################################

for site in wonderware_sites:
   
    site_source = '{}/{}/{}'.format(source, 'wonderware', site)
    path_md = '{}/{}'.format(site_source, 'Master Data.xlsx')
    
    try:
        filter_date = site_settings.loc[
            site_settings['Site'] == site, 'Start Date'].item()
        filter_date = to_date(lit(filter_date), 'd-M-yyyy')
    except:
        filter_date = to_date(lit('01-01-2019'), 'dd-MM-yyyy')
    
    
    ## Import and to spark dataframe ##########################################

    in_availability = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'availabilitydata_rpt.csv'))
    
    in_rate = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'ratedata_rpt.csv'))
    
    in_po = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'podata_rpt.csv'))
        
    in_ve = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'vareffdata_rpt.csv'))
      
    in_opn = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'opndata_rpt.csv'))
    
    in_nr_1_mseg = spark.read.option("header", True).csv(
        '{}/{}'.format(source, 'RFM_MES_MSEG.csv'))
        

    mpc_md = pd.read_excel(path_md, sheet_name='MPC', engine='openpyxl')
    mpc_md = spark.createDataFrame(mpc_md)

    category_md = pd.read_excel(path_md, sheet_name='OEE Category', engine='openpyxl')
    category_md = category_md.astype(str)
    category_md = spark.createDataFrame(category_md)
    
    shift_md = pd.read_excel(path_md, sheet_name='Shift Pattern', engine='openpyxl')
    shift_md = spark.createDataFrame(shift_md)
    

    ## Drop columns ############################################################

    in_availability = in_availability.drop(
        'UnitId', 'ShiftId', 'RunningStateID', 'ReasonID', 'ReasonGroup1ID',
        'ReasonGroup2ID', 'SequenceNo', 'TransactionId'
    )
    
    category_md = category_md.drop('Running State', 'category')
    in_ve = in_ve.drop('RecId', 'act_finish_time_local',  
                      'TransactionId')

    ## Rename columns ##########################################################

    in_availability = to_snake_case(in_availability)
    in_availability = (
        in_availability
        .withColumnRenamed('processcell', 'process_cell')
        .withColumnRenamed('eventtime', 'event_time')
        .withColumnRenamed('processorderno', 'process_order_number')
        .withColumnRenamed('targetprodrate', 'target_prod_rate')
        .withColumnRenamed('runningstate', 'running_state')
        .withColumnRenamed('reasongroupdesc2', 'reason_group_desc_2')
        .withColumnRenamed('duration', 'duration_s')
        .withColumnRenamed('targetduration', 'target_duration')
        .withColumnRenamed('stepnumber', 'step_number')
        .withColumnRenamed('stepdesc', 'step_description')
        .withColumnRenamed('comment', 'explanation')
        .withColumnRenamed('reasongroupdesc1', 'reason_group_desc_1')
    )
    
    in_rate = to_snake_case(in_rate)
    in_rate = (
        in_rate
        .withColumnRenamed('processcell', 'process_cell')
        .withColumnRenamed('processorderno', 'process_order_number')
        .withColumnRenamed('startts', 'start_ts')
        .withColumnRenamed('quantityprod', 'quantity_prod')
        .withColumnRenamed('quantitygood', 'quantity_good')
        .withColumnRenamed('targetprodrate', 'target_prod_rate')
        .withColumnRenamed('runtime', 'runtime_sec')
        .withColumnRenamed('downtime', 'downtime_sec')
    )
    
    in_po = to_snake_case(in_po)
    in_po = (
        in_po
        .withColumnRenamed('processcell', 'process_cell')
        .withColumnRenamed('processorderno', 'process_order_number')
        .withColumnRenamed('quantityprod', 'quantity_prod')
        .withColumnRenamed('targetprodrate', 'target_prod_rate')
        .withColumnRenamed('actualstarttime', 'actual_start_time')
        .withColumnRenamed('actualendtime', 'actual_end_time')
        .withColumnRenamed('materialgrade', 'material_grade')
        .withColumnRenamed('materialcolor', 'material_color')
        .withColumnRenamed('quantityrequired', 'quantity_required')
        .withColumnRenamed('quantityprod_erp', 'quantity_prod_erp')
        .withColumnRenamed('ftr_perc', 'ftr_%')
        .withColumnRenamed('rawmaterials_erp', 'raw_materials_erp')
        .withColumnRenamed('intramaterial_erp', 'intra_material_erp')
        .withColumnRenamed('freeforuse_erp', 'free_for_use_erp')
        .withColumnRenamed('targetrate', 'target_rate')
        .withColumnRenamed('targetavailability', 'target_availability')
        .withColumnRenamed('targetquality', 'target_quality')
        .withColumnRenamed('targetsetuptime', 'target_setup_time')
        .withColumnRenamed('defaultvaluesused', 'default_values_used')
        .withColumnRenamed('logsheetnum', 'logsheet_number')
    )
    
    in_ve = to_snake_case(in_ve)
    in_ve = (
        in_ve
        .withColumnRenamed('processcell', 'process_cell')
        .withColumnRenamed('processorderno', 'process_order_number')
    )
    
    in_opn = to_snake_case(in_opn)
    in_opn = (
        in_opn
        .withColumnRenamed('processcell', 'process_cell')
        .withColumnRenamed('processorderno', 'process_order_number')
        .withColumnRenamed('kopactived', 'kop_activated')
        .withColumnRenamed('alarmduration', 'alarm_duration')
        .withColumnRenamed('alarmtext', 'alarm_text')
    )
    
    mpc_md = to_snake_case(mpc_md)
    
    category_md = to_snake_case(category_md)

    shift_md = to_snake_case(shift_md)
    shift_md = shift_md.withColumnRenamed('modulo', 'shift_modulo')

    in_nr_1_mseg = to_snake_case(in_nr_1_mseg)
    

    ## Cast data types ########################################################
    
    in_availability = (
        in_availability
        .withColumn('recid', col('recid').cast('int'))
        .withColumn('event_time', 
                    to_timestamp('event_time', 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('target_prod_rate', col('target_prod_rate').cast('float'))
        .withColumn('duration_s', col('duration_s').cast('int'))
        .withColumn('target_duration', col('target_duration').cast('int'))
        .withColumn('step_number', col('step_number').cast('int'))
        .withColumn('isprocessed', col('isprocessed').cast('boolean'))
    )
    
    in_rate = (
        in_rate
        .withColumn('recid', col('recid').cast('int'))
        .withColumn('start_ts', 
                    to_timestamp(col('start_ts'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('capacity', col('capacity').cast('float'))
        .withColumn('quantity_prod', col('quantity_prod').cast('float'))
        .withColumn('quantity_good', col('quantity_good').cast('float'))
        .withColumn('target_prod_rate', col('target_prod_rate').cast('float'))
        .withColumn('runtime_sec', col('runtime_sec').cast('int'))
        .withColumn('downtime_sec', col('downtime_sec').cast('int'))
        .withColumn('isprocessed', col('isprocessed').cast('boolean'))
    )
    
    in_po = (
        in_po
        .withColumn('recid', col('recid').cast('int'))
        .withColumn('bottleneck', col('bottleneck').cast('boolean'))
        .withColumn('quantity_prod', col('quantity_prod').cast('float'))
        .withColumn('target_prod_rate', col('target_prod_rate').cast('float'))
        .withColumn('actual_start_time', 
                    to_timestamp(col('actual_start_time'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('actual_end_time', 
                    to_timestamp(col('actual_end_time'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('quantity_required', 
                    col('quantity_required').cast('float'))
        .withColumn('quantity_prod_erp',
                    col('quantity_prod_erp').cast('float'))
        .withColumn('ftr_erp', col('ftr_erp').cast('float'))
        .withColumn('ftr_%', col('ftr_%').cast('float'))
        .withColumn('raw_materials_erp', col('raw_materials_erp').cast('float'))
        .withColumn('blocked_erp', col('blocked_erp').cast('float'))
        .withColumn('rework_erp', col('rework_erp').cast('float'))
        .withColumn('scrap_erp', col('scrap_erp').cast('float'))
        .withColumn('obsolete_erp', col('obsolete_erp').cast('float'))
        .withColumn('intra_material_erp', col('intra_material_erp').cast('float'))
        .withColumn('free_for_use_erp', col('free_for_use_erp').cast('float'))
        .withColumn('target_rate', col('target_rate').cast('float'))
        .withColumn('target_availability', 
                    col('target_availability').cast('float'))
        .withColumn('target_quality', col('target_quality').cast('float'))
        .withColumn('target_setup_time', col('target_setup_time').cast('float'))
        .withColumn('default_values_used', 
                    col('default_values_used').cast('boolean'))
        .withColumn('isprocessed', col('isprocessed').cast('boolean'))
    )
    
    in_ve = (
        in_ve
        .withColumn('releasets', 
                    to_timestamp(col('releasets'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('ve_bom', col('ve_bom').cast('float'))
        .withColumn('ve_quantityproduced', 
                    col('ve_quantityproduced').cast('float'))
        .withColumn('ve_dosedfeeders', col('ve_dosedfeeders').cast('float'))
        .withColumn('ve_dosedscanned', col('ve_dosedscanned').cast('float'))
        .withColumn('ve_check', col('ve_check').cast('boolean'))
    )
    
    in_opn = (
        in_opn
        .withColumn('hourstartts', 
                    to_timestamp(col('hourstartts'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('eventts', 
                    to_timestamp(col('eventts'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('interval', col('interval').cast('int'))
        .withColumn('kop_activated', col('kop_activated').cast('int'))
        .withColumn('alarm_duration', col('alarm_duration').cast('int'))
        .withColumn('releasets', 
                    to_timestamp(col('releasets'), 'yyyy-MM-dd HH:mm:ss'))
    )

    mpc_md = (
        mpc_md
        .withColumn('default_bottleneck', 
                    col('default_bottleneck').cast('boolean'))
        .withColumn('mpc', col('mpc').cast('int'))
    )
    
    in_nr_1_mseg = (
        in_nr_1_mseg
        .withColumn('menge', col('menge').cast('float'))
        .withColumn('erfmg', col('erfmg').cast('float'))
        .withColumn('pbamg', col('pbamg').cast('float'))
        .withColumn('budat_mkpf', to_date(col('budat_mkpf'), 'yyyy-MM-dd'))
        .withColumn('cpudt_mkpf', to_date(col('cpudt_mkpf'), 'yyyy-MM-dd'))
    )

    
    ## Fix missing values #####################################################

    category_md = category_md.replace('nan', '')
    category_md = category_md.fillna('')
    
    in_availability = in_availability.fillna(
        '', 
        subset=['reason', 'reason_group_desc_1', 'reason_group_desc_2', 
                'process_order_number']
    )
    in_availability = in_availability.replace(
        'NULL', '', subset=['reason_group_desc_2'])
        
    in_po = in_po.fillna(
        '', subset=['material_grade', 'material_color', 'process_order_number'])

    in_rate = in_rate.fillna(
        '', 
        subset=['process_order_number']
    )
                
    shift_md = shift_md.replace('nan', '')


    ## in_nr1_mserg ###########################################################
        
    in_nr_1_mseg = trim_spaces(in_nr_1_mseg)
    in_nr_1_mseg = in_nr_1_mseg.withColumn(
        'process_order_number', expr("ltrim('0', rtrim(aufnr))"))
    
    
    ## in_availability #########################################################

    # add columns
    in_availability = (
        in_availability
        .withColumn('calendar_day', 
                    (col('event_time') - expr('INTERVAL 6 HOURS')).cast("date"))
        .withColumn('explanation', split(col('explanation'), r'/').getItem(0))
        .withColumn('data_refresh_dt', current_timestamp())
    )
    
    # find latest recid by grouping
    latest_recid = (
        in_availability
        .sort('recid', ascending=False)
        .drop_duplicates(['enterprise', 'site', 'plant', 'process_cell', 
                          'unit', 'calendar_day'])
        .select('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                'calendar_day', 'releasets')
    )
    
    # filter latest recid by inner join
    in_availability = in_availability.join(
        latest_recid, how='inner', 
        on=['enterprise', 'site', 'plant', 'process_cell', 'unit', 
            'calendar_day', 'releasets']
    )
    
    # take highest recid per group
    in_availability = (
        in_availability
        .sort('recid', ascending=False)
        .drop_duplicates(['enterprise', 'site', 'plant', 'process_cell', 
                          'unit', 'event_time'])
    )
    
    # create window for duration_s
    w3 = Window.partitionBy('unit').orderBy('unit', 'event_time', 'recid')
    
    # update duration_s
    in_availability = in_availability.withColumn(
        'duration_s',
        lead('event_time', 1).over(w3).cast('int')
        - col('event_time').cast('int')
    )
    

    # ## in_availability insert and update #######################################

    in_availability = in_availability.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit', 
        'event_time', 
        'process_order_number',
        'target_prod_rate',
        'shift',
        'running_state',
        'reason',
        'reason_group_desc_2',
        'duration_s',
        'target_duration',
        'step_number',
        'step_description',
        'explanation',
        'category',
        'reason_group_desc_1',
        'releasets',
        'isprocessed',
        'calendar_day',
        'data_refresh_dt'
    )

    try:
        in_availability_hist = spark.read.parquet(
            '{}/{}_{}/'.format(destination, site, 'in_availability'))
            
        # union historical data, and keep first rows by group
        print("1")
        in_availability = (
            in_availability
            .union(in_availability_hist)
            .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell', 
                                     'unit', 'event_time'])
        )
        
        # update cache and trigger action as a fix for read/write to same location
        in_availability.cache().count()
    except:
        print('No availability history')
        
    in_availability.write.mode("overwrite").parquet(
        '{}/{}_{}/'.format(destination, site, 'in_availability'))


    # ## in_rate #################################################################
    
    # add_columns
    in_rate = (
        in_rate
        .withColumn('calendar_day', 
                    (col('start_ts') - expr('INTERVAL 6 HOURS')).cast('date'))
        .withColumn('data_refresh_dt', current_timestamp())
    )
    
    # take highest releasets by grouping
    in_rate = (
        in_rate
        .sort('releasets', ascending=False)
        .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell',
                                  'unit', 'process_order_number', 'start_ts'])
    )


    # ## in_rate insert and update ###############################################
    
    in_rate = in_rate.select(
        'recid',
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'process_order_number',
        'start_ts',
        'capacity',
        'quantity_prod',
        'quantity_good',
        'target_prod_rate',
        'runtime_sec',
        'downtime_sec',
        'releasets',
        'isprocessed',
        'calendar_day',
        'data_refresh_dt'
    )

    try:
        in_rate_hist = spark.read.parquet(
            '{}/{}_{}/'.format(destination, site, 'in_rate'))
        print("2")
        in_rate = (
            in_rate
            .union(in_rate_hist)
            .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell', 
                                     'unit', 'process_order_number', 'start_ts'])
        )
        
        # update cache and trigger action as a fix for read/write to same location
        in_rate.cache().count()
    except:
        print('No rate history')
        
    in_rate.write.mode("overwrite").parquet(
        '{}/{}_{}/'.format(destination, site, 'in_rate'))


    ## in_po ###################################################################
    
    # add columns
    in_po = (
        in_po
        .withColumn('calendar_day', 
                    (col('actual_end_time') 
                    - expr('INTERVAL 6 HOURS')).cast("date"))
        .withColumn('data_refresh_dt', current_timestamp())
    )
    
    # filter calendar_day
    in_po = in_po.filter(col('calendar_day') >= filter_date)
    
    # take highest releasets by grouping
    in_po = (
        in_po
        .sort('releasets', ascending=False)
        .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell', 
                                 'unit', 'process_order_number'])
    )
    
    ## in_po insert and update #################################################

    in_po = in_po.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'process_order_number',
        'productid',
        'quantity_prod',
        'target_prod_rate',
        'actual_start_time',
        'actual_end_time',
        'material_grade',
        'material_color',
        'quantity_required',
        'quantity_prod_erp',
        'ftr_erp',
        'ftr_%',
        'raw_materials_erp',
        'blocked_erp',
        'rework_erp',
        'scrap_erp',
        'obsolete_erp',
        'intra_material_erp',
        'free_for_use_erp',
        'target_rate',
        'target_availability',
        'target_quality',
        'target_setup_time',
        'default_values_used',
        'bottleneck',
        'logsheet_number',
        'releasets',
        'isprocessed',
        'calendar_day',
        'data_refresh_dt'
    )

    try:
        in_po_hist = spark.read.parquet(
            '{}/{}_{}/'.format(destination, site, 'in_po'))
        print("3")    
        in_po = (
            in_po
            .union(in_po_hist)
            .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell', 
                                     'unit', 'process_order_number'])
        )
        
        # update cache and trigger action as a fix for read/write to same location
        in_po.cache().count()
    except:
        print('No po history')
        
    in_po.write.mode("overwrite").parquet(
        '{}/{}_{}/'.format(destination, site, 'in_po'))


    ## in_vareff ###############################################################
    
    in_ve = in_ve.withColumn(
        'data_refresh_dt', current_timestamp())
    
    w5 = (
        Window
        .partitionBy('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                     'process_order_number', 've_machine')
        .orderBy(desc("releasets"))
    )

    in_ve = (
        in_ve
        .sort('releasets', ascending=False)
        .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell', 
                                 'unit', 'process_order_number', 've_machine'])
    )
    
    
    ## in_vareff insert and update #############################################

    in_ve = in_ve.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'process_order_number',
        'releasets',
        've_machine',
        'materialnumber',
        've_materialdescription',
        've_bom',
        've_quantityproduced',
        've_dosedfeeders',
        've_dosedscanned',
        've_check',
        've_explanation',
        've_remark',
        'data_refresh_dt'
    )
    
    try:
        in_ve_hist = spark.read.parquet(
            '{}/{}_{}/'.format(destination, site, 'in_ve'))
        print("4")
        in_ve = (
            in_ve
            .union(in_ve_hist)
            .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell', 
                                     'unit', 'process_order_number', 've_machine'])
        )
    
        # update cache and trigger action as a fix for read/write to same location
        in_ve.cache().count()
    except:
        print('No ve history')
        
    in_ve.write.mode("overwrite").parquet(
        '{}/{}_{}/'.format(destination, site, 'in_ve'))


    ## in_opn ##################################################################

    in_opn = (
        in_opn
        .withColumn('calendar_day', 
                    (col('eventts') - expr('INTERVAL 6 HOURS')).cast("date"))
        .withColumn('data_refresh_dt', current_timestamp())
    )

    # filter eventtype
    in_opn = in_opn.filter(col('eventtype') != '')
    
    in_opn = (
        in_opn
        .sort('releasets', ascending=False)
        .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell', 
                                 'unit', 'kop', 'eventts'])
    )
    

    ## in_opn insert and update ################################################

    in_opn = in_opn.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'kop',
        'hourstartts',
        'eventts',
        'eventtype',
        'process_order_number',
        'interval',
        'kop_activated',
        'alarm_duration',
        'alarm_text',
        'releasets',
        'data_refresh_dt',
        'calendar_day'
     )

    try:
        in_opn_hist = spark.read.parquet(
        '{}/{}_{}/'.format(destination, site, 'in_opn'))
        print("5")
        in_opn = (
            in_opn
            .union(in_opn_hist)
            .drop_duplicates(subset=['enterprise', 'site', 'plant', 'process_cell', 
                                     'unit', 'kop', 'eventts'])
        )
    
        # update cache and trigger action as a fix for read/write to same location
        in_opn.cache().count()
    except:
        print('No opn history')
    
    in_opn.write.mode("overwrite").parquet(
        '{}/{}_{}/'.format(destination, site, 'in_opn'))
    
    
    ## ds_availability #########################################################

    # filter date
    ds_availability = in_availability.filter(
        col('calendar_day') >= filter_date)
    
    # join mpc master data
    ds_availability = ds_availability.join(
        mpc_md, how='inner', 
        on=['enterprise', 'site', 'plant', 'process_cell', 'unit']
    )
    
    # join category master data
    ds_availability = (
        ds_availability
        .join(category_md, how='inner', 
              on=['enterprise', 'site', 'reason', 'reason_group_desc_1', 
                  'reason_group_desc_2'])
    )

    # create window for updating process_order_number and target_prod_rate, step 1: ascending
    w7a = (
        Window
        .partitionBy('unit')
        .orderBy(asc("event_time"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    # update process_order_number
    ds_availability = ds_availability.withColumn(
        'process_order_number',
         when(((col('running_state') == 'IDLE') | (col('running_state') == 'CHANGE-OVER')), '-')
         .otherwise(col('process_order_number'))
    )
    
    # update target_prod_rate
    ds_availability = ds_availability.withColumn(
        'target_prod_rate',
         when(((col('running_state') == 'IDLE') | (col('running_state') == 'CHANGE-OVER')), '-')
         .otherwise(col('target_prod_rate'))
    )
    
    # empty process_order_number to nulls 
    ds_availability = ds_availability.withColumn(
        'process_order_number',
         when(col('process_order_number')=='', lit(None).cast('string'))
         .otherwise(col('process_order_number'))
    )
    
    # get the previous know value for nulls in process_order_number 
    ds_availability = ds_availability.withColumn(
        'process_order_number',
         when(col('process_order_number').isNull(), last('process_order_number', ignorenulls=True).over(w7a))
         .otherwise(col('process_order_number'))
    )
    
    # get the previous know value for nulls in target_prod_rate 
    ds_availability = ds_availability.withColumn(
        'target_prod_rate',
         when(col('target_prod_rate').isNull(), last('target_prod_rate', ignorenulls=True).over(w7a))
         .otherwise(col('target_prod_rate'))
    )

    # create window for updating process_order_number and target_prod_rate, step 2: descending
    w7b = (
        Window
        .partitionBy('unit')
        .orderBy(asc("event_time"))
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )
    
    # update process_order_number  to call IDLE as empty
    ds_availability = ds_availability.withColumn(
        'process_order_number',
         when(col('running_state') == 'IDLE',  'empty')
         .otherwise(col('process_order_number'))
    )

    # update target_prod_rate to call IDLE as empty 
    ds_availability = ds_availability.withColumn(
        'target_prod_rate',
         when(col('running_state') == 'IDLE', 'empty')
         .otherwise(col('target_prod_rate'))
    )
    
    # custom process_order_number to nulls 
    ds_availability = ds_availability.withColumn(
        'process_order_number',
         when(col('process_order_number')=='-', lit(None).cast('string'))
         .otherwise(col('process_order_number'))
    ) 
    
    # custom target_prod_rate to nulls 
    ds_availability = ds_availability.withColumn(
        'target_prod_rate',
         when(col('target_prod_rate')=='-', lit(None).cast('float'))
         .otherwise(col('target_prod_rate'))
    )
      
    # Get the previous know value for nulls in process_order_number descending  
    ds_availability = ds_availability.withColumn(
        'process_order_number',
         when(col('process_order_number').isNull(), first('process_order_number', ignorenulls=True).over(w7b))
         .otherwise(col('process_order_number'))
    )
  
    # get the previous know value for nulls in target_prod_rate descending  
    ds_availability = ds_availability.withColumn(
        'target_prod_rate',
         when(col('target_prod_rate').isNull(), first('target_prod_rate', ignorenulls=True).over(w7b))
         .otherwise(col('target_prod_rate'))
    )

    # custom value for IDLE process_order_number to null
    ds_availability = ds_availability.withColumn(
        'process_order_number',
         when(col('process_order_number')=='empty', lit(None).cast('string'))
         .otherwise(col('process_order_number'))
    )
    
    # custom value for IDLE target_prod_rate to null
    ds_availability = ds_availability.withColumn(
        'target_prod_rate',
         when(col('target_prod_rate')=='empty', lit(None).cast('float'))
         .otherwise(col('target_prod_rate').cast('float'))
    )
    
    # write availability table for checking 
    if site == 'hoek-van-holland':
        ds_availability.write.mode("overwrite").parquet(
        '{}/availability_check_temp/'.format(destination))
    
    # add columns
    ds_availability = (
        ds_availability
        .withColumn('runtime_loss', 
                    when(col('process_order_number').isNull(), 
                         col('duration_s') * col('mpc') / 3600)
                    .when(col('target_prod_rate') == 0, lit(0.0))
                    .otherwise(col('duration_s') / col('target_prod_rate') / 3600))
        .withColumn('available', 
                    when((col('running_state') == 'RUNNING')
                         | (col('running_state') == 'RUNNING SLOW')
                         | (col('running_state') == 'RUNNING FAST')
                         | (col('running_state') == 'DELAY'),
                         col('duration_s'))
                    .otherwise(lit(0)))
        .withColumn('non_impactable',
                    when((col('running_state') != 'DELAY')
                         & (col('category') == 'Non_impactable'),
                         col('duration_s'))
                    .otherwise(lit(0)))
        .withColumn('delay-ext',
                    when((col('running_state') == 'DELAY')
                         & (col('category') == 'Non_impactable'),
                         col('duration_s'))
                    .otherwise(lit(0.0)))
        .withColumn('impactable', 
                    col('duration_s') - col('available') 
                    - col('non_impactable'))
        .withColumn('costed',
                    when((col('running_state') == 'DELAY')
                         & (col('iscosted') == 'Costed'), 
                         col('duration_s'))
                    .otherwise(lit(0)))
        .withColumn('non-costed',
                    when((col('running_state') != 'DELAY')
                         | (col('iscosted') == 'Costed'), 
                         lit(0))
                    .otherwise(col('duration_s')))
        .withColumn('running_potential', 
                    when(col('available') > 0, col('runtime_loss'))
                    .otherwise(0.0))
        .withColumn('availability_loss', 
                    when(col('available') > 0, lit(0.0))
                    .otherwise(col('runtime_loss')))
        .withColumn('unknown_loss', 
                    when(col('oee_category') == 'Unknown', col('runtime_loss'))
                    .otherwise(lit(0.0)))
        .withColumn('external_loss',
                    when((col('oee_category') == 'External')
                         & (col('running_state') != 'DELAY'),
                         col('runtime_loss'))
                    .otherwise(lit(0.0)))
        .withColumn('delay-ext_loss',
                    when((col('oee_category') == 'External')
                         & (col('running_state') == 'Delay'),
                         col('runtime_loss'))
                    .otherwise(lit(0.0)))
        .withColumn('maintenance_loss',
                    when(col('oee_category') == 'Maintenance', 
                         col('runtime_loss').cast('int'))
                    .otherwise(lit(0)))
        .withColumn('operations_loss',
                    when(col('oee_category') == 'Operations', 
                         col('runtime_loss'))
                    .otherwise(lit(0.0)))
    )

    # select columns of in_po to join
    in_po_avail = (
        in_po
        .filter((col('calendar_day') > filter_date) 
                & (col('process_order_number') != '0000000'))
        .select(col('process_order_number'), col('unit'), col('productid'),
                col('material_grade'), col('material_color'), 
                col('bottleneck'), col('logsheet_number'))
    )

    # join selected columns of in_po
    ds_availability = (
        ds_availability
        .join(in_po_avail.withColumn('right', lit(True)), 
              how='left', 
              on=['process_order_number', 'unit'])
        .fillna(False, subset='right')
        .withColumn('productid',
                    when(~col('right'), lit('UNKNOWN MATERIAL'))
                    .otherwise(col('productid')))
        .withColumn('material_grade',
                    when(~col('right'), lit('Unknown'))
                    .otherwise(col('material_grade')))
        .withColumn('material_color', 
                    when(~col('right'), lit('Unknown'))
                    .otherwise(col('material_color')))
        .withColumn('bottleneck', 
                    when(~col('right'), col('default_bottleneck'))
                    .otherwise(col('bottleneck')))
        .withColumn('process_order_number', 
                    when(~col('right'), lit('0000000'))
                    .otherwise(col('process_order_number')))
        .drop('right')
    )

    # add column
    ds_availability = ds_availability.withColumn(
        'shift_modulo', 
        datediff(to_date(lit('2019-10-01'), 'yyyy-MM-dd'), col('calendar_day')) % 14
    )

    # join shift md
    ds_availability = (
        ds_availability
        .join(shift_md, how='inner', on=['shift', 'shift_modulo'])
        .drop('shift')
        .withColumnRenamed('shift_letter', 'shift')
    )

    # ## ds_rt_ftr ################################################################
    
    # filter date
    ds_rt_ftr = in_rate.filter(
        col('calendar_day') >= filter_date)
    
    # join mpc_md
    ds_rt_ftr = (
        ds_rt_ftr
        .drop('releasesets', 'isprocessed', 'data_refresh_dt')
        .join(mpc_md.drop('mpc'), how='inner', 
              on=['enterprise', 'site', 'plant', 'process_cell', 'unit'])
    )

    # select columns from in_po to join
    in_po_rt_ftr = (
        in_po
        .filter((col('calendar_day') >= filter_date)
                & (col('process_order_number') != '0000000'))
        .select('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                'process_order_number', 'productid', 'material_grade', 
                'material_color', 'ftr_%', 'bottleneck', 'logsheet_number')
    )

    # join selected columns in_po
    ds_rt_ftr = (
        ds_rt_ftr
        .join(in_po_rt_ftr.withColumn('right', lit(True)), 
              how='left', 
              on=['enterprise', 'site', 'plant', 'process_cell', 'unit',
                  'process_order_number'])
        .fillna(False, subset='right')
        .withColumn('process_order_number',
                    when(~col('right'), lit('0000000'))
                    .otherwise(col('process_order_number')))
        .withColumn('quantity_ftr', 
                    when(~col('right'), 
                         col('quantity_prod'))
                    .otherwise(col('ftr_%') / 100 * col('quantity_prod')))
        .withColumn('ftr_%', 
                    when(~col('right'), lit(100.0))
                    .otherwise(col('ftr_%')))
        .withColumn('productid', 
                    when(~col('right'), lit('UNKNOWN_MATERIAL'))
                    .otherwise(col('productid')))
        .withColumn('material_grade', 
                    when(~col('right'), lit('Unknown'))
                    .otherwise(col('material_grade')))
        .withColumn('material_color', 
                    when(~col('right'), lit('Unknown'))
                    .otherwise(col('material_color')))
        .withColumn('bottleneck', 
                    when(~col('right'), col('default_bottleneck'))
                    .otherwise(col('bottleneck')))
        .drop('right')
    )
    
    
    # ## ds_oee ##################################################################

    ds_avail_agg = (
        ds_availability
        .groupby('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                 'calendar_day', 'process_order_number', 'productid', 
                 'material_grade', 'material_color', 'bottleneck')
        .agg(sum('runtime_loss').alias('sum_runtime_loss'),
             sum('available').alias('sum_available'),
             sum('non_impactable').alias('sum_non_impactable'),
             sum('impactable').alias('sum_impactable'),
             sum('running_potential').alias('sum_running_potential'),
             sum('availability_loss').alias('sum_availability_loss'),
             sum('unknown_loss').alias('sum_unknown_loss'),
             sum('external_loss').alias('sum_external_loss'),
             sum('maintenance_loss').alias('sum_maintenance_loss'),
             sum('operations_loss').alias('sum_operations_loss'),
             sum('costed').alias('sum_costed'),
             sum('non-costed').alias('sum_non-costed'),
             sum('delay-ext_loss').alias('sum_delay-ext_loss'))
    )
    
    ds_rt_ftr_agg = (
        ds_rt_ftr
        .groupby('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                 'calendar_day', 'process_order_number', 'productid', 
                 'material_grade', 'material_color', 'bottleneck')
        .agg(sum('capacity').alias('sum_capacity'),
             sum('quantity_prod').alias('sum_quantity_prod'),
             sum('quantity_ftr').alias('sum_quantity_ftr'),
             sum('runtime_sec').alias('sum_runtime_sec'),
             sum('downtime_sec').alias('sum_downtime_sec'))
        .drop('productid', 'material_grade', 'material_color', 'bottleneck')
    )

    ds_oee = (
        ds_avail_agg
        .join(ds_rt_ftr_agg, how='left', 
              on=['enterprise', 'site', 'plant', 'process_cell',  'unit', 
                  'calendar_day', 'process_order_number'])
        .fillna(0, subset=['sum_capacity', 'sum_quantity_prod', 
                          'sum_quantity_ftr', 'sum_runtime_sec',
                          'sum_downtime_sec'])
        .withColumn('sum_quantity_non-ftr',
                    col('sum_quantity_prod') - col('sum_quantity_ftr'))
        .withColumn('sum_rate_gap', 
                    col('sum_runtime_loss') - col('sum_quantity_prod')
                    - col('sum_operations_loss') - col('sum_maintenance_loss')
                    - col('sum_external_loss') - col('sum_unknown_loss'))
        .withColumn('sum_capacity',
                    col('sum_running_potential') - col('sum_delay-ext_loss'))
        .drop('sum_delay-ext_loss')
    )
    

    ## ds_ve ###################################################################
    
    in_po_unique = (
        in_po
        .filter((col('calendar_day') >= filter_date) 
                & col('bottleneck'))
        .select('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                'calendar_day', 'process_order_number', 'productid',
                'bottleneck', 'logsheet_number')
        .drop_duplicates()
    )
    
    in_nr_1_mseg_filter = (
        in_nr_1_mseg
        .filter(
            col('bwart').isin(['101', '102', '261', '262'])
            & col('meins').isin(['LB', 'KG'])
            & ~col('process_order_number').isNull())
        .withColumnRenamed('process_order_number', 'process_order_number_right')
    )
    

    ## ds_ve ##################################################################

    ds_ve = (
        in_ve
        .drop('enterprise', 'site', 'plant', 'process_cell', 'unit', 'releasets')
        .join(in_po_unique, how='inner', on='process_order_number')
    )
    
    cond1 = [
        col('process_order_number') == col('process_order_number_right'), 
        col('materialnumber') == col('matnr')
    ]
    
    ds_ve = ds_ve.join(in_nr_1_mseg_filter, how='outer', on=cond1)
    

    ## ds_ve manipulate based on join #########################################

    ds_ve_middle = ds_ve.filter(
        ~col('process_order_number').isNull()
        & ~col('process_order_number_right').isNull()
    )

    ds_ve_left_t = ds_ve.filter(
        ~col('process_order_number').isNull()
        & col('process_order_number_right').isNull()
        & (col('unit') == col('ve_machine'))
    )

    ds_ve_left_f = ds_ve.filter(
        ~col('process_order_number').isNull()
        & col('process_order_number_right').isNull()
        & (col('unit') != col('ve_machine'))
    )

    ds_ve_right_t = ds_ve.filter(
        col('process_order_number').isNull()
        & ~col('process_order_number_right').isNull()
        & col('bwart').isin(['101', '102'])
    )
    
    ds_ve_right_f = ds_ve.filter(
        col('process_order_number').isNull()
        & ~col('process_order_number_right').isNull()
        & ~col('bwart').isin(['101', '102'])
    )

    cond2 = [
        col('process_order_number') == col('process_order_number_right')]


    ds_ve_1 = (
        ds_ve_middle
        .withColumn('insmk', lit(None).cast('string'))
        .drop('ve_dosedscanned')
        .withColumnRenamed('menge', 've_dosedscanned')
        .select(
            'process_order_number', 'process_cell', 've_machine',
            'materialnumber', 've_materialdescription', 've_bom', 
            've_quantityproduced', 've_dosedfeeders', 've_dosedscanned', 
            've_check', 've_explanation', 've_remark', 
            'data_refresh_dt', 
            'enterprise', 'site', 'plant', 'unit', 
            'calendar_day', 'productid', 'bottleneck', 'logsheet_number', 
            'bwart', 'insmk', 'meins',  
            'process_order_number_right'
        )
    )

    ds_ve_2 = (
        ds_ve_left_f
        .withColumn('insmk', lit(None).cast('string'))
        .select(
            'process_order_number', 'process_cell', 've_machine',
            'materialnumber', 've_materialdescription', 've_bom', 
            've_quantityproduced', 've_dosedfeeders', 've_dosedscanned', 
            've_check', 've_explanation', 've_remark',
            'data_refresh_dt', 
            'enterprise', 'site', 'plant', 'unit', 
            'calendar_day', 'productid', 'bottleneck', 'logsheet_number', 
            'bwart', 'insmk', 'meins',  
            'process_order_number_right'
        )
    )

    ds_ve_3 = (
        ds_ve_left_t
        .drop('ve_dosedscanned', 'process_order_number_right', 'bwart', 
              'menge', 'meins')
        .join(ds_ve_right_t
              .select('bwart', 'menge', 'meins', 'process_order_number_right')
              .withColumnRenamed('menge', 've_dosedscanned'),
              how='inner', on=cond2)
        .withColumn('insmk', lit(None).cast('string'))
        .select(
            'process_order_number', 'process_cell', 've_machine',
            'materialnumber', 've_materialdescription', 've_bom', 
            've_quantityproduced', 've_dosedfeeders', 've_dosedscanned', 
            've_check', 've_explanation', 've_remark', 
            'data_refresh_dt', 
            'enterprise', 'site', 'plant', 'unit', 
            'calendar_day', 'productid', 'bottleneck', 'logsheet_number', 
            'bwart', 'insmk', 'meins',  
            'process_order_number_right'
        )
    )
    
    ds_ve_4 = (
        ds_ve_left_t
        .drop('process_order_number_right', 'bwart', 'menge', 'meins')
        .join(ds_ve_right_t
              .select('process_order_number_right', 'bwart', 'menge', 'meins'), 
              how='left', on=cond2)
        .filter(col('process_order_number_right').isNull())
        .withColumn('insmk', lit(None).cast('string'))
        .select(
            'process_order_number', 'process_cell', 've_machine',
            'materialnumber', 've_materialdescription', 've_bom', 
            've_quantityproduced', 've_dosedfeeders', 've_dosedscanned', 
            've_check', 've_explanation', 've_remark', 
            'data_refresh_dt', 
            'enterprise', 'site', 'plant', 'unit', 
            'calendar_day', 'productid', 'bottleneck', 'logsheet_number', 
            'bwart', 'insmk', 'meins',  
            'process_order_number_right'
        )
    )
    
    cond3 = [
        col('process_order_number_right') == col('process_order_number')]
    ds_ve_5 = (
        ds_ve_right_f
        .select('bwart', 'matnr', 'insmk', 'menge', 'meins', 'process_order_number_right')
        .withColumnRenamed('matnr', 've_materialdescription')
        .withColumnRenamed('menge', 've_dosedscanned')
        .join(in_po_unique, how='inner', on=cond3)
        .withColumn('ve_machine', lit(None).cast('string'))
        .withColumn('materialnumber', lit(None).cast('string'))
        .withColumn('ve_bom', lit(None).cast('float'))
        .withColumn('ve_quantityproduced', lit(None).cast('float'))
        .withColumn('ve_dosedfeeders', lit(None).cast('float'))
        .withColumn('ve_check', lit(None).cast('boolean'))
        .withColumn('ve_explanation', lit(None).cast('string'))
        .withColumn('ve_remark', lit(None).cast('string'))
        .withColumn('data_refresh_dt', to_timestamp(lit('')))
        .select(
            'process_order_number', 'process_cell', 've_machine',
            'materialnumber', 've_materialdescription', 've_bom', 
            've_quantityproduced', 've_dosedfeeders', 've_dosedscanned', 
            've_check', 've_explanation', 've_remark', 
            'data_refresh_dt', 
            'enterprise', 'site', 'plant', 'unit', 
            'calendar_day', 'productid', 'bottleneck', 'logsheet_number', 
            'bwart', 'insmk', 'meins',  
            'process_order_number_right'
        )
    )     
    
    ds_ve = ds_ve_1
    ds_ve = ds_ve.union(ds_ve_2)
    ds_ve = ds_ve.union(ds_ve_3)
    ds_ve = ds_ve.union(ds_ve_4)
    ds_ve = ds_ve.union(ds_ve_5)
    
    
    ## ds_mseg ################################################################

    ds_mseg = in_po_unique.join(
        in_nr_1_mseg.drop('insmk'), 
        how='inner', 
        on='process_order_number'
    )
    
    ds_mseg_unique = (
        ds_mseg
        .filter(col('bwart') == '101')
        .select('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                'calendar_day', 'process_order_number', 'productid', 
                'matnr', 'charg', 'bottleneck', 'logsheet_number')
        .drop_duplicates()
    )
    
    ds_mseg_charg = (
        ds_mseg_unique
        .join(in_nr_1_mseg.drop('process_order_number','matnr'), 
              how='inner', on=['charg'])
        .select('process_order_number', 'enterprise', 'site', 'plant', 
                'process_cell', 'unit', 'calendar_day', 'productid', 
                'bottleneck', 'logsheet_number', 'mblnr', 'zeile', 
                'bwart', 'xauto', 'matnr', 'werks', 'lgort', 'charg', 
                'shkzg', 'bwtar', 'menge', 'meins', 'erfmg', 'erfme', 
                'equnr', 'aufnr', 'pbamg', 'ummat', 'umwrk', 'umlgo', 
                'umcha', 'umbar', 'kzbew', 'kzvbr', 'kzzug', 'weunb', 
                'bwlvs', 'budat_mkpf', 'cpudt_mkpf', 'cputm_mkpf', 
                'usnam_mkpf', 'insmk')
    )
    
    
    if "mjahr" in ds_mseg.columns:
        ds_mseg = ds_mseg.drop("mjahr")
    
    ds_mseg_umcha = (
        ds_mseg_unique
        .withColumnRenamed('charg', 'umcha')
        .join(in_nr_1_mseg.drop('insmk', 'process_order_number', 'matnr'), 
              how='inner', on=['umcha'])
        .select('process_order_number', 'enterprise', 'site', 'plant', 
                'process_cell', 'unit', 'calendar_day', 'productid', 
                'bottleneck', 'logsheet_number', 'mblnr', 'zeile', 
                'bwart', 'xauto', 'matnr', 'werks', 'lgort', 'charg', 
                'shkzg', 'bwtar', 'menge', 'meins', 'erfmg', 'erfme', 
                'equnr', 'aufnr', 'pbamg', 'ummat', 'umwrk', 'umlgo', 
                'umcha', 'umbar', 'kzbew', 'kzvbr', 'kzzug', 'weunb', 
                'bwlvs', 'budat_mkpf', 'cpudt_mkpf', 'cputm_mkpf', 
                'usnam_mkpf')
    )
    ds_mseg = ds_mseg.union(ds_mseg_charg.drop('insmk'))
    ds_mseg = ds_mseg.union(ds_mseg_umcha)

    ds_mseg = (
        ds_mseg
        .sort('mblnr', 'zeile', 'process_order_number')
        .drop_duplicates(subset=['mblnr', 'zeile'])
    )


    ## ds_mseg_q1 #############################################################

    ds_mseg_sort = (
        ds_mseg_charg
        .select('process_order_number', 'budat_mkpf')
        .drop_duplicates()
        .sort('process_order_number', 'budat_mkpf')
        .withColumnRenamed('budat_mkpf', 'first_booking_date')
        .drop_duplicates(subset=['process_order_number'])
    )

    ds_mseg_q1 = ds_mseg_charg.join(
        ds_mseg_sort, on='process_order_number')
        
    ds_mseg_q1 = ds_mseg_q1.filter(
        (col('xauto') != 'X') | col('xauto').isNull())
    
    # add columns
    ds_mseg_q1 = (
        ds_mseg_q1
        .withColumn('menge-s', 
                    when(col('shkzg') == 'S', col('menge'))
                    .otherwise(-1 * col('menge')))
        .withColumn('in_q1_time', 
                    datediff(col('budat_mkpf'), 
                             col('first_booking_date')) <= 7)
        .withColumn('quantity_prod-erp', 
                    when(col('in_q1_time') 
                         & ((col('bwart') == '101') | (col('bwart') == '102')),
                         col('menge-s'))
                    .otherwise(lit(0.0)))
        .withColumn('quantity_q1_good-erp1', 
                    when(col('in_q1_time') 
                         & ((col('bwart') == '321') | (col('bwart') == '342') 
                            | (col('bwart') == '343')),
                         col('menge'))
                    .otherwise(lit(0.0)))
        .withColumn('quantity_q1_good-erp2', 
                    when(col('in_q1_time') 
                         & ((col('bwart') == '322') | (col('bwart') == '341') 
                            | (col('bwart') == '344')),
                         -1 * col('menge'))
                    .otherwise(lit(0.0)))
        .withColumn('quantity_q1_good-erp3', 
                    when(col('in_q1_time') 
                         & ((col('bwart') == '101') | (col('bwart') == '102'))
                         & ((col('insmk') == 'F') | col('insmk').isNull()),
                         col('menge-s'))
                    .otherwise(lit(0.0)))
        .withColumn('quantity_q1_good-erp', 
                    col('quantity_q1_good-erp1') + col('quantity_q1_good-erp2')
                    + col('quantity_q1_good-erp3'))
        .withColumn('quantity_q1_bad-erp', 
                    col('quantity_prod-erp') - col('quantity_q1_good-erp'))
        .drop('insmk', 'quantity_q1_good-erp1', 'quantity_q1_good-er2', 
              'quantity_q1_good-erp3')
    )    


    ## ds_opn #################################################################

    ds_opn = (
        in_opn
        .drop('releasesets')
        .filter(col('calendar_day') >= filter_date)
        .join(in_po
              .select('process_order_number', 'unit', 'productid', 
                      'material_grade', 'material_color', 'bottleneck', 
                      'logsheet_number'),
              how='inner',
              on=['process_order_number', 'unit'])
    )


    ## Select columns #########################################################

    ds_availability = ds_availability.select(
        'shift',
        'category',
        'oee_category',
        'iscosted',
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'event_time',
        'process_order_number',
        'target_prod_rate',
        'running_state',
        'reason',
        'reason_group_desc_2',
        'duration_s',
        'explanation',
        'reason_group_desc_1',
        'calendar_day',
        'target_duration',
        'step_number',
        'step_description',
        'mpc',
        'default_bottleneck',
        'runtime_loss',
        'available',
        'non_impactable',
        'impactable',
        'costed',
        'non-costed',
        'running_potential',
        'availability_loss',
        'unknown_loss',
        'external_loss',
        'maintenance_loss',
        'operations_loss',
        'productid',
        'material_grade',
        'material_color',
        'bottleneck',
        'logsheet_number'
    )
    
    ds_mseg = ds_mseg.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'calendar_day',
        'process_order_number',
        'productid',
        'logsheet_number',
        'bottleneck',
        'mblnr',
        'aufnr',
        'xauto',
        'zeile',
        'weunb',
        'umlgo',
        'meins',
        'menge',
        'lgort',
        'pbamg',
        'kzzug',
        'kzvbr',
        'kzbew',
        'budat_mkpf',
        'umcha',
        'shkzg',
        'werks',
        'bwart',
        'charg',
        'cputm_mkpf',
        'cpudt_mkpf',
        'umwrk',
        'ummat',
        'erfme',
        'erfmg',
        'matnr',
        'usnam_mkpf'
    )
    
    ds_mseg_q1 = ds_mseg_q1.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'calendar_day',
        'process_order_number',
        'productid',
        'bottleneck',
        'logsheet_number',
        'mblnr',
        'aufnr',
        'xauto',
        'zeile',
        'weunb',
        'umlgo',
        'meins',
        'menge',
        'lgort',
        'pbamg',
        'kzzug',
        'kzvbr',
        'kzbew',
        'budat_mkpf',
        'umcha',
        'shkzg',
        'werks',
        'bwart',
        'charg',
        'cputm_mkpf',
        'cpudt_mkpf',
        'umwrk',
        'ummat',
        'erfme',
        'erfmg',
        'matnr',
        'usnam_mkpf',
        'first_booking_date',
        'menge-s',
        'in_q1_time',
        'quantity_prod-erp',
        'quantity_q1_good-erp',
        'quantity_q1_bad-erp'
    )
    
    ds_oee = ds_oee.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'calendar_day',
        'process_order_number',
        'productid',
        'material_grade',
        'material_color',
        'bottleneck',
        'sum_runtime_loss',
        'sum_available',
        'sum_non_impactable',
        'sum_impactable',
        'sum_running_potential',
        'sum_availability_loss',
        'sum_unknown_loss',
        'sum_external_loss',
        'sum_maintenance_loss',
        'sum_operations_loss',
        'sum_costed',
        'sum_non-costed',
        'sum_capacity',
        'sum_quantity_prod',
        'sum_quantity_ftr',
        'sum_runtime_sec',
        'sum_downtime_sec',
        'sum_quantity_non-ftr',
        'sum_rate_gap'
    )
    
    ds_opn = ds_opn.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'kop',
        'hourstartts',
        'eventts',
        'eventtype',
        'process_order_number',
        'interval',
        'kop_activated',
        'alarm_duration',
        'alarm_text',
        'data_refresh_dt',
        'calendar_day',
        'productid',
        'material_grade',
        'material_color',
        'bottleneck',
        'logsheet_number'
    )
    
    in_po = in_po.select(
        'process_order_number',
        'productid',
        'unit',
        'actual_start_time',
        'actual_end_time',
        'calendar_day',
        'target_prod_rate',
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'bottleneck',
        'ftr_%',
        'logsheet_number',
        'material_grade',
        'material_color',
        'blocked_erp',
        'quantity_prod_erp',
        'raw_materials_erp',
        'ftr_erp',
        'rework_erp',
        'scrap_erp',
        'obsolete_erp',
        'intra_material_erp',
        'free_for_use_erp',
        'default_values_used',
        'quantity_prod',
        'quantity_required',
        'target_setup_time',
        'target_rate',
        'target_availability',
        'target_quality'        
    )
    
    ds_rt_ftr = ds_rt_ftr.select(
        'process_order_number',
        'productid',
        'unit',
        'calendar_day',
        'target_prod_rate',
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'bottleneck',
        'ftr_%',
        'logsheet_number',
        'material_grade',
        'material_color',
        'default_bottleneck',
        'downtime_sec',
        'runtime_sec',
        'quantity_prod',
        'quantity_good',
        'quantity_ftr',
        'start_ts',
        'capacity'
    )
    
    ds_ve = ds_ve.select(
        've_machine',
        'materialnumber',
        've_materialdescription',
        've_bom',
        've_quantityproduced',
        've_dosedfeeders',
        've_check',
        've_explanation',
        've_remark',
        'data_refresh_dt',
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'calendar_day',
        'process_order_number',
        'productid',
        'bottleneck',
        'logsheet_number',
        'bwart',
        've_dosedscanned',
        'meins',
        'insmk'
    )
    
    
    ## Append dataframe to list ###############################################

    availability_list.append(ds_availability)
    mseg_list.append(ds_mseg)
    mseg_q1_list.append(ds_mseg_q1)
    oee_list.append(ds_oee)
    opn_list.append(ds_opn)
    po_list.append(in_po)
    rt_ftr_list.append(ds_rt_ftr)
    ve_list.append(ds_ve)



###############################################################################
## AspenTech transformations for each site ####################################
###############################################################################

for site in aspentech_sites:
    
    site_source = '{}/{}/{}'.format(source, 'aspentech', site)
    
    
    ## Import and convert to Spark dataframe ######################################
    
    # mes data
    duration = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'Duration.csv'))
        
    explanation = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'Explanation.csv'))      

    operation = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'Operation.csv'))
            
    operation_duration = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'Operation Duration.csv'))
    
    procedure = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'Procedure.csv'))
    
    unit_procedure = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'Unit procedure.csv'))

    yield_ = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'Yield.csv'))

for site in aspentech_sites_unapproved:
  
    unapproved_site_source = '{}/{}/{}/{}'.format(source, 'aspentech', site, 'unapproved')
    
    ## Adding the lines below to skip the folder which doensn't have unapproved subfolder
    response_check = s3_client.list_objects_v2(Bucket= bucket_name, Prefix = unapproved_site_source)
    
    if not 'Contents' in response_check.keys():
        continue
    ## Import and convert to Spark dataframe ######################################
    
    # mes data
    unapproved_duration = spark.read.option("header", True).csv(
        '{}/{}'.format(unapproved_site_source, 'Duration.csv'))
        
    unapproved_explanation = spark.read.option("header", True).csv(
        '{}/{}'.format(unapproved_site_source, 'Explanation.csv'))      

    unapproved_operation = spark.read.option("header", True).csv(
        '{}/{}'.format(unapproved_site_source, 'Operation.csv'))
            
    unapproved_operation_duration = spark.read.option("header", True).csv(
        '{}/{}'.format(unapproved_site_source, 'Operation Duration.csv'))
    
    unapproved_procedure = spark.read.option("header", True).csv(
        '{}/{}'.format(unapproved_site_source, 'Procedure.csv'))
    
    unapproved_unit_procedure = spark.read.option("header", True).csv(
        '{}/{}'.format(unapproved_site_source, 'Unit procedure.csv'))

    unapproved_yield_ = spark.read.option("header", True).csv(
        '{}/{}'.format(unapproved_site_source, 'Yield.csv'))
       
    # sap data 
    in_nr_1_mseg = spark.read.option("header", True).csv(
        '{}/{}'.format(source, 'RFM_MES_MSEG.csv'))  
    
    # master data
    path_md = '{}/{}'.format(site_source, 'AT MasterData.xlsx')
    
    operations_md = pd.read_excel(path_md, sheet_name="Operations Master Data",  
                                  engine='openpyxl')
    operations_md['ProcedureArea'] = operations_md['ProcedureArea'].astype(str)
    operations_md['Step Description'] = operations_md['Step Description'].astype(str)
    operations_md['Type'] = operations_md['Type'].astype(str)
    operations_md = spark.createDataFrame(operations_md)
    operations_md = operations_md.withColumnRenamed('Type', 'operationtype')
    
    reporting_cat_md = pd.read_excel(path_md, sheet_name="Reporting Category",  
                                     engine='openpyxl')
    reporting_cat_md['Subcategory'] = reporting_cat_md['Subcategory'].astype(str)
    reporting_cat_md['Category'] = reporting_cat_md['Category'].astype(str)
    reporting_cat_md['Group'] = reporting_cat_md['Group'].astype(str)
    reporting_cat_md = spark.createDataFrame(reporting_cat_md)
    
    s95_md = pd.read_excel(path_md, sheet_name = "S95",  engine='openpyxl')
    s95_md = spark.createDataFrame(s95_md)


    ## Drop columns ###############################################################
    
    operations_md = operations_md.drop('ProcedureArea')
    reporting_cat_md = reporting_cat_md.drop('ProcedureArea')
    in_nr_1_mseg = in_nr_1_mseg.drop('XSAUF', 'BWTAR', 'EQUNR', 'UMBAR', 'BWLVS')

    
    ## Rename columns #############################################################
    
    # columns to snake_case
    duration = to_snake_case(duration)
    explanation = to_snake_case(explanation)
    operation = to_snake_case(operation)
    operation_duration = to_snake_case(operation_duration)
    procedure = to_snake_case(procedure)
    unit_procedure = to_snake_case(unit_procedure)
    yield_ = to_snake_case(yield_)
    operations_md = to_snake_case(operations_md)
    reporting_cat_md = to_snake_case(reporting_cat_md)
    s95_md = to_snake_case(s95_md)
    in_nr_1_mseg = to_snake_case(in_nr_1_mseg)
	
	# columns to snake_case unapproved
	unapproved_duration = to_snake_case(unapproved_duration)
    unapproved_explanation = to_snake_case(unapproved_explanation)
    unapproved_operation = to_snake_case(unapproved_operation)
    unapproved_operation_duration = to_snake_case(unapproved_operation_duration)
    unapproved_procedure = to_snake_case(unapproved_procedure)
    unapproved_unit_procedure = to_snake_case(unapproved_unit_procedure)
    unapproved_yield_ = to_snake_case(unapproved_yield_)
    
    
    prefix_yield = ['id', 'actual', 'cost', 'loss', 'target']
    for c in prefix_yield:
         yield_ = yield_.withColumnRenamed(c, "yield"+c)
    
    for c in procedure.columns:
         procedure = procedure.withColumnRenamed(c, "procedure"+c)
         
    procedure = procedure.withColumnRenamed(
        'procedureproduction_order', 'process_order_number')
    procedure = procedure.withColumnRenamed(
        'procedureproduct_name', 'productid')
        
    prefix_operation = ['id', 'start', 'end']
    for c in prefix_operation:
        operation = operation.withColumnRenamed(c, "operation"+c)
        
    operation = operation.withColumnRenamed('name', 'step_description')
    
    prefix_explanation = ['id', 'instance', 'cost', 'loss', 'object', 'group', 
                          'category', 'subcategory', 'remark', 
                          'batchtimelosscategory']
    for c in prefix_explanation:
        explanation = explanation.withColumnRenamed(c, "explanation"+c)
    
    for c in duration.columns:
        duration = duration.withColumnRenamed(c, "duration"+c)
    
    prefix_unit_procedure = ['id', 'name', 'start', 'end', 'reactorvolume']    
    for c in prefix_unit_procedure:
        unit_procedure = unit_procedure.withColumnRenamed(c, "unit_procedure"+c)
    
    prefix_reporting_cat_md = ['category', 'subcategory', 'group']
    for c in prefix_reporting_cat_md:
        reporting_cat_md = reporting_cat_md.withColumnRenamed(c, "explanation"+c)
		
	unapproved_prefix_yield = ['id', 'actual', 'cost', 'loss', 'target']
    for c in unapproved_prefix_yield:
         unapproved_yield_ = unapproved_yield_.withColumnRenamed(c, "yield"+c)
    
    for c in unapproved_procedure.columns:
         unapproved_procedure = unapproved_procedure.withColumnRenamed(c, "procedure"+c)
    
	##unapproved
	
    unapproved_procedure = unapproved_procedure.withColumnRenamed(
        'procedureproduction_order', 'process_order_number')
    unapproved_procedure = unapproved_procedure.withColumnRenamed(
        'procedureproduct_name', 'productid')
        
    unapproved_prefix_operation = ['id', 'start', 'end']
    for c in unapproved_prefix_operation:
        unapproved_operation = unapproved_operation.withColumnRenamed(c, "operation"+c)
        
    unapproved_operation = unapproved_operation.withColumnRenamed('name', 'step_description')
    
    unapproved_prefix_explanation = ['id', 'instance', 'cost', 'loss', 'object', 'group', 
                          'category', 'subcategory', 'remark', 
                          'batchtimelosscategory']
    for c in unapproved_prefix_explanation:
        unapproved_explanation = unapproved_explanation.withColumnRenamed(c, "explanation"+c)
    
    for c in unapproved_duration.columns:
        unapproved_duration = unapproved_duration.withColumnRenamed(c, "duration"+c)
    
    unapproved_prefix_unit_procedure = ['id', 'name', 'start', 'end', 'reactorvolume']    
    for c in unapproved_prefix_unit_procedure:
        unapproved_unit_procedure = unapproved_unit_procedure.withColumnRenamed(c, "unit_procedure"+c)
    
    # rename columns
    reporting_cat_md = reporting_cat_md.withColumnRenamed(
        'reporting_category', 'oee_category')
    s95_md = s95_md.withColumnRenamed('unit_procedureunit', 'unit')
    
    
    ## Drop empty rows ############################################################

    procedure = procedure.filter(~col('procedurearea').isNull())
    explanation = explanation.filter(~col('explanationcost').isNull())
	
	unapproved_procedure = unapproved_procedure.filter(~col('procedurearea').isNull())
    unapproved_explanation = unapproved_explanation.filter(~col('explanationcost').isNull())
    
    
    ## Cast columns ###############################################################
    
    duration = (
        duration
        .withColumn('durationid', col('durationid').cast('int'))
        .withColumn('durationactual', col('durationactual').cast('int'))
        .withColumn('durationcost', col('durationcost').cast('float'))
        .withColumn('durationloss', col('durationloss').cast('int'))
        .withColumn('durationtarget', col('durationtarget').cast('int'))
        .withColumn('durationgm', col('durationgm').cast('float'))
    )
    
    explanation = (
        explanation
        .withColumn('explanationid', col('explanationid').cast('int'))
        .withColumn('explanationinstance', col('explanationinstance').cast('int'))
        .withColumn('explanationcost', col('explanationcost').cast('float'))
        .withColumn('explanationloss', col('explanationloss').cast('int'))
    )    
    
    operation = (
        operation
        .withColumn('operationid', col('operationid').cast('int'))
        .withColumn('unit_procedureid', col('unit_procedureid').cast('int'))
        .withColumn('operationid', col('operationid').cast('int'))
        .withColumn('operationstart', 
                    to_timestamp(date_format(format_dates(col('operationstart')), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('operationend', 
                    to_timestamp(date_format(format_dates(col('operationend')), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
    )
    
    operation_duration = (
        operation_duration
        .withColumn('operationid', col('operationid').cast('int'))
        .withColumn('durationid', col('durationid').cast('int'))
    )
    
    procedure = (
        procedure
        .withColumn('procedureid', col('procedureid').cast('int'))
        .withColumn('procedurestart', 
                    to_timestamp(date_format(format_dates(col='procedurestart'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('procedureend', 
                    to_timestamp(date_format(format_dates(col='procedureend'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
    )
    
    unit_procedure = (
        unit_procedure
        .withColumn('unit_procedureid', col('unit_procedureid').cast('int'))
        .withColumn('procedureid', col('procedureid').cast('int'))
        .withColumn('unit_procedurestart', 
                   to_timestamp(date_format(format_dates(col='unit_procedurestart'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('unit_procedureend', 
                   to_timestamp(date_format(format_dates(col='unit_procedureend'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('unit_procedurereactorvolume', 
                    col('unit_procedurereactorvolume').cast('float'))
    )

    yield_ = (
        yield_
        .withColumn('yieldid', col('yieldid').cast('int'))
        .withColumn('procedureid', col('procedureid').cast('int'))
        .withColumn('yieldactual', col('yieldactual').cast('float'))
        .withColumn('yieldcost', col('yieldcost').cast('float'))
        .withColumn('yieldloss', col('yieldloss').cast('float'))
        .withColumn('yieldtarget', col('yieldtarget').cast('float'))
    )
    
    
    operations_md = (
        operations_md
        .withColumn('step_number', col('step_number').astype('int'))
    )

    s95_md = (
        s95_md
        .withColumn('mpc', col('mpc').cast('int'))
    )

    in_nr_1_mseg = (
        in_nr_1_mseg
        .withColumn('menge', col('menge').cast('float'))
        .withColumn('erfmg', col('erfmg').cast('float'))
        .withColumn('pbamg', col('pbamg').cast('float'))
        .withColumn('budat_mkpf', to_date(col('budat_mkpf'), 'yyyy-MM-dd'))
        .withColumn('cpudt_mkpf', to_date(col('cpudt_mkpf'), 'yyyy-MM-dd'))
    )
    
	## Cast unapproved columns ###############################################################
    
    unapproved_duration = (
        unapproved_duration
        .withColumn('durationid', col('durationid').cast('int'))
        .withColumn('durationactual', col('durationactual').cast('int'))
        .withColumn('durationcost', col('durationcost').cast('float'))
        .withColumn('durationloss', col('durationloss').cast('int'))
        .withColumn('durationtarget', col('durationtarget').cast('int'))
        .withColumn('durationgm', col('durationgm').cast('float'))
    )
    
    unapproved_explanation = (
        unapproved_explanation
        .withColumn('explanationid', col('explanationid').cast('int'))
        .withColumn('explanationinstance', col('explanationinstance').cast('int'))
        .withColumn('explanationcost', col('explanationcost').cast('float'))
        .withColumn('explanationloss', col('explanationloss').cast('int'))
    )    
    
    unapproved_operation = (
        unapproved_operation
        .withColumn('operationid', col('operationid').cast('int'))
        .withColumn('unit_procedureid', col('unit_procedureid').cast('int'))
        .withColumn('operationid', col('operationid').cast('int'))
        .withColumn('operationstart', 
                    to_timestamp(date_format(format_dates(col('operationstart')), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('operationend', 
                    to_timestamp(date_format(format_dates(col('operationend')), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
    )
    
    unapproved_operation_duration = (
        unapproved_operation_duration
        .withColumn('operationid', col('operationid').cast('int'))
        .withColumn('durationid', col('durationid').cast('int'))
    )
    
    unapproved_procedure = (
        unapproved_procedure
        .withColumn('procedureid', col('procedureid').cast('int'))
        .withColumn('procedurestart', 
                    to_timestamp(date_format(format_dates(col='procedurestart'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('procedureend', 
                    to_timestamp(date_format(format_dates(col='procedureend'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
    )
    
    unapproved_unit_procedure = (
        unapproved_unit_procedure
        .withColumn('unit_procedureid', col('unit_procedureid').cast('int'))
        .withColumn('procedureid', col('procedureid').cast('int'))
        .withColumn('unit_procedurestart', 
                   to_timestamp(date_format(format_dates(col='unit_procedurestart'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('unit_procedureend', 
                   to_timestamp(date_format(format_dates(col='unit_procedureend'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))
        .withColumn('unit_procedurereactorvolume', 
                    col('unit_procedurereactorvolume').cast('float'))
    )

    unapproved_yield_ = (
        unapproved_yield_
        .withColumn('yieldid', col('yieldid').cast('int'))
        .withColumn('procedureid', col('procedureid').cast('int'))
        .withColumn('yieldactual', col('yieldactual').cast('float'))
        .withColumn('yieldcost', col('yieldcost').cast('float'))
        .withColumn('yieldloss', col('yieldloss').cast('float'))
        .withColumn('yieldtarget', col('yieldtarget').cast('float'))
    )
	
    ## Fix missing values #########################################################

    reporting_cat_md = reporting_cat_md.replace('nan', '')
    explanation = explanation.fillna(
        "",
        subset=['explanationcategory', 'explanationsubcategory', 
                'explanationgroup']
    )

    ## Add columns ################################################################
    
    in_nr_1_mseg = in_nr_1_mseg.withColumn(
        'process_order_number', expr("ltrim('0', rtrim(aufnr))"))

    
    ## Join tables ################################################################

    merge_1_1 = procedure.join(yield_, on='procedureid', how='left')

    merge_1_2 = operation.join(operations_md, on='step_description', how='inner')
    
    cond_1_3 = ['explanationcategory', 'explanationsubcategory', 
                'explanationgroup']
    
    merge_1_3 = explanation.join(reporting_cat_md, on=cond_1_3, how='inner')
    merge_1_3 = merge_1_3.replace('', None, subset=cond_1_3)

    merge_2 = duration.join(merge_1_3, on='durationid', how = 'left')

    merge_3 = operation_duration.join(merge_2, on='durationid', how = 'inner')

    merge_4 = merge_1_2.join(merge_3, on='operationid', how = 'inner')

    merge_5 = unit_procedure.join(merge_4, on='unit_procedureid', how = 'inner')

    merge_6 = merge_1_1.join(merge_5, on='procedureid', how = 'inner')
    
    cond_7 = ['procedurearea', 'unit']
    merge_7 = merge_6.join(s95_md, on=cond_7, how='inner')
    
	## Join unapproved tables ################################################################

    unapproved_merge_1_1 = unapproved_procedure.join(unapproved_yield_, on='procedureid', how='left')

    unapproved_merge_1_2 = unapproved_operation.join(operations_md, on='step_description', how='inner')
    
    unapproved_cond_1_3 = ['explanationcategory', 'explanationsubcategory', 
                'explanationgroup']
    
    unapproved_merge_1_3 = unapproved_explanation.join(reporting_cat_md, on=unapproved_cond_1_3, how='inner')
    unapproved_merge_1_3 = unapproved_merge_1_3.replace('', None, subset=unapproved_cond_1_3)

    unapproved_merge_2 = unapproved_duration.join(unapproved_merge_1_3, on='durationid', how = 'left')

    unapproved_merge_3 = unapproved_operation_duration.join(unapproved_merge_2, on='durationid', how = 'inner')

    unapproved_merge_4 = unapproved_merge_1_2.join(unapproved_merge_3, on='operationid', how = 'inner')

    unapproved_merge_5 = unapproved_unit_procedure.join(unapproved_merge_4, on='unit_procedureid', how = 'inner')

    unapproved_merge_6 = unapproved_merge_1_1.join(unapproved_merge_5, on='procedureid', how = 'inner')
    
    unapproved_cond_7 = ['procedurearea', 'unit']
    unapproved_merge_7 = unapproved_merge_6.join(s95_md, on=unapproved_cond_7, how='inner')

    # fill na
    fill_0 = ['yieldid', 'yieldactual', 'yieldlcost', 'yieldloss', 'yieldtarget', 
              'explanationid', 'explanationinstance', 'explanationcost', 
              'explanationloss']
    merged = merge_7.na.fill(value=0, subset=fill_0)
    # fill_str = ['explanationremark', 'explanationbatchtimelosscategory']
    # merged = merged.na.fill(value="", subset=fill_str)
	
	# fill na unapproved
    unapproved_fill_0 = ['yieldid', 'yieldactual', 'yieldlcost', 'yieldloss', 'yieldtarget', 
              'explanationid', 'explanationinstance', 'explanationcost', 
              'explanationloss']
    unapproved_merged = unapproved_merge_7.na.fill(value=0, subset=unapproved_fill_0)
    # fill_str = ['explanationremark', 'explanationbatchtimelosscategory']
    # merged = merged.na.fill(value="", subset=fill_str)
    
    ## Add columns ################################################################
    
    enriched = merged.withColumn(
        "procedurecalendar_day", 
        (col('procedureend') - expr('INTERVAL 7 HOURS')).cast("date")
    )
    
    enriched = enriched.withColumn(
        "unit_procedurecalendar_day", 
        (col('unit_procedureend') - expr('INTERVAL 7 HOURS')).cast("date")
    )
    
    enriched = enriched.withColumn(
        'operation_calendar_day', 
        (col('operationend') - expr('INTERVAL 7 HOURS')).cast("date"))
     
    enriched = enriched.withColumn(
        "lossexternal", 
        when(col('oee_category') == 'External', col('explanationloss'))
        .otherwise(0)
    )
    
    enriched = enriched.withColumn(
        "lossoperations", 
        when(col('oee_category') == 'Operations', col('explanationloss'))
        .otherwise(lit(0.0))
    )
    
    enriched = enriched.withColumn(
        "lossmaintenance", 
        when(col('oee_category') == 'Maintenance', col('explanationloss'))
        .otherwise(lit(0.0))
    )
    
	## Add columns ################################################################
    
    unapproved_enriched = unapproved_merged.withColumn(
        "procedurecalendar_day", 
        (col('procedureend') - expr('INTERVAL 7 HOURS')).cast("date")
    )
    
    unapproved_enriched = unapproved_enriched.withColumn(
        "unit_procedurecalendar_day", 
        (col('unit_procedureend') - expr('INTERVAL 7 HOURS')).cast("date")
    )
    
    unapproved_enriched = unapproved_enriched.withColumn(
        'operation_calendar_day', 
        (col('operationend') - expr('INTERVAL 7 HOURS')).cast("date"))
     
    unapproved_enriched = unapproved_enriched.withColumn(
        "lossexternal", 
        when(col('oee_category') == 'External', col('explanationloss'))
        .otherwise(0)
    )
    
    unapproved_enriched = unapproved_enriched.withColumn(
        "lossoperations", 
        when(col('oee_category') == 'Operations', col('explanationloss'))
        .otherwise(lit(0.0))
    )
    
    unapproved_enriched = unapproved_enriched.withColumn(
        "lossmaintenance", 
        when(col('oee_category') == 'Maintenance', col('explanationloss'))
        .otherwise(lit(0.0))
    )

    ## Approved Aggregated data 1 ###########################################################
    
    groupby_1_cols = [
        'enterprise', 'site', 'plant', 'process_cell', 'bottleneck', 'mpc', 
        'procedureid', 'procedurearea', 'process_order_number', 'productid',
        'procedureproduct_group', 'procedurerecipe_code', 
        'procedurestart', 'procedureend', 'procedurecalendar_day', 'yieldtarget',
        'yieldactual', 'unit_procedureid', 'unit_procedurename', 
        'unit', 'unit_procedurestart', 'Unit_procedureend', 
        'unit_procedurecalendar_day', 'operationid', 'step_description', 
        'operationtype', 'operationstart', 'operationend', 'operation_calendar_day', 
        'durationid', 'durationactual', 'durationcost', 'durationloss', 
        'durationtarget', 'durationgm'
    ]
    
    approved_aggregated_1 = (
        enriched
        .groupBy(groupby_1_cols)
        .agg(sum('explanationcost').alias('sum_explanationcost'),
             sum('explanationloss').alias('sum_explanationloss'),
             sum('lossexternal').alias('sum_lossexternal'),
             sum('lossoperations').alias('sum_lossoperations'),
             sum('lossmaintenance').alias('sum_lossmaintenance'))
    )
	
	## Unapproved Aggregated data 1 ###########################################################
    
    unapproved_groupby_1_cols = [
        'enterprise', 'site', 'plant', 'process_cell', 'bottleneck', 'mpc', 
        'procedureid', 'procedurearea', 'process_order_number', 'productid',
        'procedureproduct_group', 'procedurerecipe_code', 
        'procedurestart', 'procedureend', 'procedurecalendar_day', 'yieldtarget',
        'yieldactual', 'unit_procedureid', 'unit_procedurename', 
        'unit', 'unit_procedurestart', 'Unit_procedureend', 
        'unit_procedurecalendar_day', 'operationid', 'step_description', 
        'operationtype', 'operationstart', 'operationend', 'operation_calendar_day', 
        'durationid', 'durationactual', 'durationcost', 'durationloss', 
        'durationtarget', 'durationgm'
    ]
    
    unapproved_aggregated_1 = (
        unapproved_enriched
        .groupBy(unapproved_groupby_1_cols)
        .agg(sum('explanationcost').alias('sum_explanationcost'),
             sum('explanationloss').alias('sum_explanationloss'),
             sum('lossexternal').alias('sum_lossexternal'),
             sum('lossoperations').alias('sum_lossoperations'),
             sum('lossmaintenance').alias('sum_lossmaintenance'))
    )
	

## Add columns to approved_aggregated_1 ################################################
    
    approved_aggregated_1 = approved_aggregated_1.withColumn('durationidle', col('sum_lossexternal'))
    
    approved_aggregated_1 = approved_aggregated_1.withColumn(
        'durationrunning',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(least(col('durationactual') - col('sum_lossexternal'), 
                         col('durationtarget')))
    )
    
    approved_aggregated_1 = approved_aggregated_1.withColumn(
        'durationextratime',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(greatest(col('durationactual') 
                            - col('durationtarget') 
                            - col('sum_lossexternal'),
                            lit(0)))
    ) 
    
    approved_aggregated_1 = approved_aggregated_1.withColumn(
        'durationrunningtarget',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(col('durationtarget'))
    )
    
    approved_aggregated_1 = approved_aggregated_1.withColumn(
        'durationstopped',
        when(col('operationtype') == 'Stopped', 
             greatest(col('durationactual') - col('sum_lossexternal'), lit(0)))
        .otherwise(lit(0))
    )
	
	## Add columns to aggregated 1 ################################################
    
    unapproved_aggregated_1 = unapproved_aggregated_1.withColumn('durationidle', col('sum_lossexternal'))
    
    unapproved_aggregated_1 = unapproved_aggregated_1.withColumn(
        'durationrunning',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(least(col('durationactual') - col('sum_lossexternal'), 
                         col('durationtarget')))
    )
    
    unapproved_aggregated_1 = unapproved_aggregated_1.withColumn(
        'durationextratime',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(greatest(col('durationactual') 
                            - col('durationtarget') 
                            - col('sum_lossexternal'),
                            lit(0)))
    ) 
    
    unapproved_aggregated_1 = unapproved_aggregated_1.withColumn(
        'durationrunningtarget',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(col('durationtarget'))
    )
    
    unapproved_aggregated_1 = unapproved_aggregated_1.withColumn(
        'durationstopped',
        when(col('operationtype') == 'Stopped', 
             greatest(col('durationactual') - col('sum_lossexternal'), lit(0)))
        .otherwise(lit(0))
    )
	
	#UNION
	union_agg_1 = approved_aggregated_1.union(unapproved_aggregated_1)
    
    
	## Approved Aggregated data 2 ###########################################################
    
    groupby_2_cols = [
        'enterprise', 'site', 'plant', 'process_cell', 'bottleneck', 'mpc', 
        'procedureid', 'procedurearea', 'productid',
        'process_order_number', 'procedureproduct_group', 
        'procedurerecipe_code', 'procedurestart', 'procedureend', 
        'procedurecalendar_day', 'yieldtarget', 'yieldactual', 'unit_procedureid', 
        'unit_procedurename', 'unit', 'unit_procedurestart', 'unit_procedureend', 
        'unit_procedurecalendar_day', 
    ]
    
    approved_aggregated_2 = (
        approved_aggregated_2
        .groupBy(groupby_2_cols)
        .agg(sum('durationactual').alias('sum_durationactual'), 
             sum('durationloss').alias('sum_durationloss'),
             sum('durationtarget').alias('sum_durationtarget'),
             sum('sum_lossexternal').alias('sum_sum_lossexternal'), 
             sum('sum_lossoperations').alias('sum_sum_lossoperations'), 
             sum('sum_lossmaintenance').alias('sum_sum_lossmaintenance'), 
             sum('durationrunning').alias('sum_durationrunning'), 
             sum('durationextratime').alias('sum_durationextratime'), 
             sum('durationrunningtarget').alias('sum_durationrunningtarget'), 
             sum('durationstopped').alias('sum_durationstopped'))
    )
	
	## Unapproved Aggregated data 2 ###########################################################
    
    unapproved_groupby_2_cols = [
        'enterprise', 'site', 'plant', 'process_cell', 'bottleneck', 'mpc', 
        'procedureid', 'procedurearea', 'productid',
        'process_order_number', 'procedureproduct_group', 
        'procedurerecipe_code', 'procedurestart', 'procedureend', 
        'procedurecalendar_day', 'yieldtarget', 'yieldactual', 'unit_procedureid', 
        'unit_procedurename', 'unit', 'unit_procedurestart', 'unit_procedureend', 
        'unit_procedurecalendar_day', 
    ]
    
    unapproved_aggregated_2 = (
        unapproved_aggregated_2
        .groupBy(unapproved_groupby_2_cols)
        .agg(sum('durationactual').alias('sum_durationactual'), 
             sum('durationloss').alias('sum_durationloss'),
             sum('durationtarget').alias('sum_durationtarget'),
             sum('sum_lossexternal').alias('sum_sum_lossexternal'), 
             sum('sum_lossoperations').alias('sum_sum_lossoperations'), 
             sum('sum_lossmaintenance').alias('sum_sum_lossmaintenance'), 
             sum('durationrunning').alias('sum_durationrunning'), 
             sum('durationextratime').alias('sum_durationextratime'), 
             sum('durationrunningtarget').alias('sum_durationrunningtarget'), 
             sum('durationstopped').alias('sum_durationstopped'))
    )
    

	## Add columns to approved_aggregated_2 ################################################
    
    approved_aggregated_2 = approved_aggregated_2.withColumn(
        'sum_durationrunningtotal',
        col('sum_durationrunning') + col('sum_durationextratime')
    )
    
    approved_aggregated_2 = approved_aggregated_2.withColumn(
        'target_prod_rate',
        when((col('yieldtarget') == 0) | (col('yieldtarget').isNull()), 
             1 / col('mpc'))
        .otherwise(col('sum_durationrunningtarget') / 60 / col('yieldtarget'))
    )
    
    approved_aggregated_2 = approved_aggregated_2.withColumn(
        'target_prod_rate_po',
        when((col('yieldtarget') == 0) | (col('yieldtarget').isNull()), col('mpc'))
        .otherwise(col('yieldtarget') / col('sum_durationrunningtarget') * 60)
    )
    
    approved_aggregated_2 = approved_aggregated_2.withColumn('ftr_%', lit(100.0))
    approved_aggregated_2 = approved_aggregated_2.withColumn('logsheet_number', lit(None).cast('string'))
    approved_aggregated_2 = approved_aggregated_2.withColumn('material_grade', lit(None).cast('string'))
    approved_aggregated_2 = approved_aggregated_2.withColumn('material_color', lit(None).cast('string'))
    approved_aggregated_2 = approved_aggregated_2.withColumn('quantity_prod', col('yieldactual'))
    approved_aggregated_2 = approved_aggregated_2.withColumn(
        'default_bottleneck', lit(None).cast('boolean'))
		
	## Add columns to unapproved_aggregated_2 ################################################
    
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn(
        'sum_durationrunningtotal',
        col('sum_durationrunning') + col('sum_durationextratime')
    )
    
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn(
        'target_prod_rate',
        when((col('yieldtarget') == 0) | (col('yieldtarget').isNull()), 
             1 / col('mpc'))
        .otherwise(col('sum_durationrunningtarget') / 60 / col('yieldtarget'))
    )
    
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn(
        'target_prod_rate_po',
        when((col('yieldtarget') == 0) | (col('yieldtarget').isNull()), col('mpc'))
        .otherwise(col('yieldtarget') / col('sum_durationrunningtarget') * 60)
    )
    
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn('ftr_%', lit(100.0))
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn('logsheet_number', lit(None).cast('string'))
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn('material_grade', lit(None).cast('string'))
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn('material_color', lit(None).cast('string'))
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn('quantity_prod', col('yieldactual'))
    unapproved_aggregated_2 = unapproved_aggregated_2.withColumn(
        'default_bottleneck', lit(None).cast('boolean'))
		
	#UNION
	
	union_agg_2 = approved_aggregated_2.union(unapproved_aggregated_2)
    

    ## Add columns to ds_at_rt_ftr ################################################
    
    ds_at_rt_ftr = aggregated.withColumn(
        'downtime_sec', col('sum_durationstopped') * 60)
    ds_at_rt_ftr = ds_at_rt_ftr.withColumn(
        'runtime_sec', col('sum_durationrunningtotal') * 60)
    ds_at_rt_ftr = ds_at_rt_ftr.withColumn('quantity_good', lit(0.0))
    ds_at_rt_ftr = ds_at_rt_ftr.withColumn('quantity_ftr', col('yieldactual'))
    ds_at_rt_ftr = ds_at_rt_ftr.withColumn('start_ts', col('unit_procedurestart'))
    ds_at_rt_ftr = ds_at_rt_ftr.withColumn(
        'capacity', 
        col('sum_durationrunningtotal') / col('target_prod_rate') / 60
    )
    
    # rename
    ds_at_rt_ftr = ds_at_rt_ftr.withColumnRenamed(
        'unit_procedurecalendar_day', 'calendar_day')
    
    ds_at_rt_ftr = ds_at_rt_ftr.select(
        'process_order_number', 'productid', 'unit', 'calendar_day', 
        'target_prod_rate', 'enterprise' , 'site', 'plant', 'process_cell', 
        'bottleneck', 'ftr_%', 'logsheet_number', 'material_grade', 
        'material_color', 'default_bottleneck', 'downtime_sec', 'runtime_sec', 
        'quantity_prod', 'quantity_good', 'quantity_ftr', 'start_ts', 'capacity'
    )
    
    
    ## Add columns to ds_at_po ####################################################
    
    ds_at_po = aggregated.withColumn('blocked_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('quantity_prod_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('raw_materials_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('ftr_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('rework_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('scrap_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('obsolete_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('intra_material_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('free_for_use_erp', lit(0.0))
    ds_at_po = ds_at_po.withColumn('default_values_used', lit(False))
    ds_at_po = ds_at_po.withColumn('quantity_required', col('yieldtarget'))
    ds_at_po = ds_at_po.withColumn('target_setup_time', lit(None).cast('float'))
    ds_at_po = ds_at_po.withColumn('target_rate', lit(0.95))
    ds_at_po = ds_at_po.withColumn('target_availability', lit(0.95))
    ds_at_po = ds_at_po.withColumn('target_quality', lit(0.95))
    
    # rename columns
    ds_at_po = (
        ds_at_po
        .withColumnRenamed('unit_procedurecalendar_day', 'calendar_day')
        .withColumnRenamed('unit_procedurestart', 'actual_start_time')
        .withColumnRenamed('unit_procedureend', 'actual_end_time')
        .drop('target_prod_rate')
        .withColumnRenamed('target_prod_rate_po', 'target_prod_rate')
    )
    
    ds_at_po = ds_at_po.select(
        'process_order_number', 'productid', 'unit', 'actual_start_time',
        'actual_end_time', 'calendar_day', 'target_prod_rate', 'enterprise' , 
        'site', 'plant', 'process_cell', 'bottleneck', 'ftr_%', 'logsheet_number',
        'material_grade', 'material_color', 'blocked_erp', 'quantity_prod_erp',
        'raw_materials_erp', 'ftr_erp', 'rework_erp', 'scrap_erp', 'obsolete_erp',
        'intra_material_erp', 'free_for_use_erp', 'default_values_used', 
        'quantity_prod', 'quantity_required', 'target_setup_time', 'target_rate',
        'target_availability', 'target_quality'
    )
    
    
    ## Expand and transform ds_at_availability ####################################
    
    # transform column
    expanded = enriched.withColumn(
        'operation_calendar_day',
        coalesce('operation_calendar_day', 'procedurecalendar_day')
    )
    
    # new column
    window_spec  = Window.partitionBy('durationid').orderBy("durationid", 'explanationid')
    expanded = expanded.withColumn(
        'deltafromstart', 
        sum('explanationloss').over(window_spec)
    )
    
    expanded = expanded.withColumn(
        'deltafromstart', 
        lag('deltafromstart').over(window_spec)
    )
    expanded = expanded.fillna(value=0, subset=['deltafromstart'])
    
    # conditions
    cond_exp0 = col("explanationinstance") == 0
    cond_exp1 = col("explanationinstance") == 1
    cond_dur = col("durationactual") <= col("durationtarget")

    # block 1
    expanded_1 = (
        expanded
        .where(cond_exp0 & cond_dur)
        .withColumn('eventduration', col('durationactual'))
        .withColumn('event_time', col('operationstart'))
        .withColumn('available', 
                    when(col('operationtype') == 'Running', 
                         col('eventduration') * 60)
                    .otherwise(lit(0)))
        .withColumn('non_impactable', lit(0))
        .withColumn('impactable', 
                    when(col('operationtype') == 'Running', lit(0))
                    .otherwise(col('eventduration') * 60))
        .withColumn('running_state', 
                    when(col('operationtype') == 'Running', lit('RUNNING'))
                    .otherwise(lit('CHANGE-OVER')))
        .withColumn('explanationsubcategory', col('step_description'))
        .withColumn('oee_category', 
                    when(col('operationtype') == 'Running', lit(None).cast('string'))
		    .otherwise(lit('Operations')))
    )
    
    # block 2.1
    expanded_2_1 = (
        expanded
        .where(cond_exp0 & ~cond_dur)
        .withColumn('eventduration', col('durationtarget'))
        .withColumn('event_time', col('operationstart'))
        .withColumn('available', 
                    when(col('operationtype') == 'Running', 
                         col('eventduration') * 60)
                    .otherwise(lit(0)))
        .withColumn('non_impactable', lit(0))
        .withColumn('impactable', 
                    when(col('operationtype') == 'Running', lit(0))
                    .otherwise(col('eventduration') * 60))
        .withColumn('running_state', 
                    when(col('operationtype') == 'Running', lit('RUNNING'))
                    .otherwise(lit('CHANGE-OVER')))
        .withColumn('explanationsubcategory', col('step_description'))
        .withColumn('oee_category', 
                    when(col('operationtype') == 'Running', lit(None).cast('string'))
		    .otherwise(lit('Operations')))
    )
    
    # block 2.2
    expanded_2_2 = (
        expanded
        .where(cond_exp0 & ~cond_dur)
        .withColumn('eventduration', col('durationactual') - col('durationtarget'))
	    .where(col('eventduration') > 0)
        .withColumn('event_time',
                    (unix_timestamp("operationstart") + col("durationtarget")*60)
                    .cast('timestamp'))
        .withColumn('explanationloss', col('eventduration'))
        .withColumn('lossoperations', col('eventduration'))
        .withColumn('explanationgroup', lit('Operations'))
        .withColumn('available', 
                    when(col('operationtype') == 'Running', 
                         col('eventduration') * 60)
                        .otherwise(lit(0)))
        .withColumn('non_impactable', lit(0))
        .withColumn('impactable', 
                    when(col('operationtype') == 'Running', lit(0))
                    .otherwise(col('eventduration') * 60))
        .withColumn('running_state', 
                    when(col('oee_category') == 'External', lit('IDLE'))
                    .when((col('operationtype') == 'Running') &
                          (col('oee_category') != 'External'), lit('DELAY'))
                    .otherwise(lit('DOWNTIME')))
        .withColumn('explanationsubcategory', col('step_description'))
        .withColumn('explanationcategory', lit('Unknown Delay'))
        .withColumn('oee_category', lit('Unknown'))
    )
    
    # block 3.1
    expanded_3_1 = (
        expanded
        .where(cond_exp1)
        .withColumn('eventduration', col('durationtarget'))
        .withColumn('event_time', col('operationstart'))
        .withColumn('explanationloss', lit(0.0))
        .withColumn('lossexternal', lit(0))
        .withColumn('lossoperations', lit(0.0))
        .withColumn('lossmaintenance', lit(0.0))
        .withColumn('explanationgroup', lit(None).cast('string'))
        .withColumn('available', 
                    when(col('operationtype') == 'Running', 
                         col('eventduration') * 60)
                        .otherwise(lit(0)))
        .withColumn('non_impactable', lit(0))
        .withColumn('impactable', 
                    when(col('operationtype') == 'Running', lit(0))
                    .otherwise(col('eventduration') * 60))
        .withColumn('running_state', 
                    when(col('operationtype') == 'Running', 'RUNNING')
                    .otherwise(lit('CHANGE-OVER')))
        .withColumn('explanationsubcategory', col('step_description'))
        .withColumn('explanationcategory', lit(None).cast('string'))
        .withColumn('explanationremark', lit(None).cast('string'))
        .withColumn('oee_category', 
                    when(col('operationtype') == 'Running', 
                         lit(None).cast('string'))
                    .otherwise(lit('Operations')))
    )
    
    # block 3.2
    expanded_3_2 = (
        expanded
        .where(cond_exp1)
        .withColumn('eventduration', col('explanationloss'))
        .withColumn('event_time',
                    (unix_timestamp("operationstart") + col("durationtarget")*60)
                    .cast('timestamp'))
        .withColumn('available', 
                    when(col('operationtype') == 'Running', 
                         col('eventduration') * 60)
                    .otherwise(lit(0)))
        .withColumn('non_impactable', 
                    when(((col('operationtype') != 'Running')
                          & (col('oee_category') == 'External')),
                         col('eventduration') * 60)
                    .otherwise(lit(0)))
        .withColumn('impactable', 
                    when((col('operationtype') != ('Running'))
                         & (col('oee_category') != ('External')),
                         col('eventduration') * 60)
                    .otherwise(lit(0)))
        .withColumn('running_state', 
                    when(col('oee_category') == 'External', lit('IDLE'))
                    .when((col('operationtype') == 'Running') &
			  (col('oee_category') != 'External'), lit('DELAY'))
                    .otherwise(lit('DOWNTIME')))
    )

    # block 4
    expanded_4 = (
        expanded
        .where(~cond_exp0 & ~cond_exp1)
        .withColumn('eventduration', col('explanationloss'))
        .withColumn('event_time',
                    (unix_timestamp("operationstart") + col("DeltaFromStart")*60)
                    .cast('timestamp'))
        .withColumn('available', 
                    when(col('operationtype') == 'Running', 
                         col('eventduration') * 60)
                    .otherwise(lit(0)))
        .withColumn('non_impactable', 
                    when(((col('operationtype') != 'Running')
                          & (col('oee_category') == 'External')),
                         col('eventduration') * 60)
                    .otherwise(lit(0)))
        .withColumn('impactable', 
                    when(((col('operationtype') != 'Running')
                          & (col('oee_category') != 'External')),
                         col('eventduration') * 60)
                    .otherwise(lit(0)))
        .withColumn('running_state', 
                    when(col('oee_category') == 'External', lit('IDLE'))
                    .when((col('operationtype') == 'Running') &
                          (col('oee_category') != 'External'), lit('DELAY'))
                    .otherwise(lit('DOWNTIME')))
    )

    # merge blocks
    expanded = expanded_1.union(expanded_2_1)
    expanded = expanded.union(expanded_2_2)
    expanded = expanded.union(expanded_3_1)
    expanded = expanded.union(expanded_3_2)
    ds_at_availability = expanded.union(expanded_4)


    ## Add target_prod_rate to ds_at_availability #################################
    
    target_prod_rate = aggregated.select(
        col('process_order_number'), col('unit'), 
        col('target_prod_rate')
    )
    
    ds_at_availability = ds_at_availability.join(
        target_prod_rate, how='inner', 
        on=['process_order_number', 'unit']
    )
    
    
    ## Add columns to ds_at_availability ##########################################
    
    ds_at_availability = ds_at_availability.withColumn(
        'shift', 
        when((hour(col('event_time')) < 6) | (hour(col('event_time')) >= 22), 'ND')
        .when(hour(col('event_time')) < 14, 'OD')
        .otherwise('MD')
    )
    
    ds_at_availability = ds_at_availability.withColumn(
        'shift_modulo', 
        datediff(to_date(lit('2017-08-19'), 'yyyy-MM-dd'),col('event_time')) % 10
    )
    
    ds_at_availability = ds_at_availability.withColumn(
        'category', when(col('oee_category') == 'External', 'Non-impactable'))
    
    ds_at_availability = ds_at_availability.withColumn(
        'duration_s', col('eventduration') * 60)
    
    ds_at_availability = ds_at_availability.withColumn(
        'material_grade', lit(None).cast('string'))
    
    ds_at_availability = ds_at_availability.withColumn(
        'material_color', lit(None).cast('string'))
    
    ds_at_availability = ds_at_availability.withColumn(
        'target_duration', col('durationtarget') * 60)
    
    ds_at_availability = ds_at_availability.withColumn(
        'runtime_loss', col('duration_s') / col('target_prod_rate') / 3600)
    
    ds_at_availability = ds_at_availability.withColumn(
        'running_potential',
        when(col('available') > 0, col('runtime_loss'))
        .otherwise(lit(0.0))
    )
    
    ds_at_availability = ds_at_availability.withColumn(
        'availability_loss',
        when(col('available') > 0, lit(0.0))
        .otherwise(col('runtime_loss'))
    )
    
    ds_at_availability = ds_at_availability.withColumn(
        'unknown_loss',
        when(col('oee_category') == 'Unknown', col('runtime_loss'))
        .otherwise(lit(0.0))
    )
    
    ds_at_availability = ds_at_availability.withColumn(
        'external_loss',
        when(col('oee_category') == 'External', col('runtime_loss'))
        .otherwise(lit(0.0))
    )
    
    ds_at_availability = ds_at_availability.withColumn(
        'maintenance_loss',
        when(col('oee_category') == 'Maintenance', 
             col('runtime_loss').cast('int'))
        .otherwise(lit(0))
    )
    
    ds_at_availability = ds_at_availability.withColumn(
        'operations_loss',
        when(col('oee_category') == 'Operations', col('runtime_loss'))
        .otherwise(lit(0.0))
    )
    
    ds_at_availability = ds_at_availability.withColumn('costed', lit(0))
    ds_at_availability = ds_at_availability.withColumn('non-costed', lit(0))
    ds_at_availability = ds_at_availability.withColumn(
        'default_bottleneck', lit(None).cast('boolean'))
    ds_at_availability = ds_at_availability.withColumn(
        'logsheet_number', lit(None).cast('string'))
    ds_at_availability = ds_at_availability.withColumn(
        'iscosted', lit(None).cast('float'))
    
    # rename
    ds_at_availability = (
        ds_at_availability
        .withColumnRenamed('operation_calendar_day', 'calendar_day')
        .withColumnRenamed('explanationgroup', 'reason_group_desc_1')
        .withColumnRenamed('explanationcategory', 'reason_group_desc_2')
        .withColumnRenamed('explanationsubcategory', 'reason')
        .withColumnRenamed('explanationremark', 'explanation')
    )
    
    # select
    ds_at_availability = ds_at_availability.select(
        'enterprise' , 'site', 'plant', 'process_cell', 'bottleneck', 'mpc',
        'process_order_number', 'productid', 'unit', 'step_description', 
        'calendar_day', 'reason_group_desc_1', 'reason_group_desc_2', 'reason',
        'explanation', 'oee_category', 'event_time', 'available', 'non_impactable',
        'impactable', 'running_state', 'step_number', 'target_prod_rate', 'shift',
        'category', 'duration_s', 'material_grade', 'material_color', 
        'target_duration', 'runtime_loss', 'running_potential', 
        'availability_loss', 'unknown_loss', 'external_loss', 'maintenance_loss',
        'operations_loss', 'costed', 'non-costed', 'default_bottleneck', 
        'logsheet_number', 'iscosted'
    )
    

    ## Create ds_at_oee ###########################################################
    
    ds_at_availability_agg = (
        ds_at_availability
        .groupby('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                 'calendar_day', 'process_order_number', 'productid', 
                 'material_grade', 'material_color', 'bottleneck')
        .agg(sum('runtime_loss').alias('sum_runtime_loss'), 
             sum('available').alias('sum_available'), 
             sum('non_impactable').alias('sum_non_impactable'), 
             sum('impactable').alias('sum_impactable'), 
             sum('running_potential').alias('sum_running_potential'), 
             sum('availability_loss').alias('sum_availability_loss'), 
             sum('unknown_loss').alias('sum_unknown_loss'), 
             sum('external_loss').alias('sum_external_loss'), 
             sum('maintenance_loss').alias('sum_maintenance_loss'), 
             sum('operations_loss').alias('sum_operations_loss'), 
             sum('costed').alias('sum_costed'), 
             sum('non-costed').alias('sum_non-costed'))
    )
    
    ds_at_rt_ftr_agg = (
        ds_at_rt_ftr
        .groupby('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                 'calendar_day', 'process_order_number')
        .agg(sum('capacity').alias('sum_capacity'), 
             sum('quantity_prod').alias('sum_quantity_prod'), 
             sum('quantity_ftr').alias('sum_quantity_ftr'), 
             sum('runtime_sec').alias('sum_runtime_sec'), 
             sum('downtime_sec').alias('sum_downtime_sec'))
    )
    
    cond = ['enterprise', 'site', 'plant', 'process_cell', 'unit', 'calendar_day', 
            'process_order_number']
    ds_at_oee = ds_at_availability_agg.join(ds_at_rt_ftr_agg, how='left', on=cond)
    
    ds_at_oee = ds_at_oee.na.fill(
        0.0, subset=['sum_capacity', 'sum_quantity_prod', 'sum_quantity_ftr', 
                     'sum_runtime_sec', 'sum_downtime_sec']
    )
    
    ds_at_oee = ds_at_oee.withColumn(
        'sum_quantity_non-ftr', 
        col('sum_quantity_prod') - col('sum_quantity_ftr')
    )
    
    ds_at_oee = ds_at_oee.withColumn(
        'sum_rate_gap', 
        col('sum_runtime_loss') 
        - col('sum_quantity_prod') 
        - col('sum_operations_loss')
        - col('sum_maintenance_loss')
        - col('sum_external_loss')
        - col('sum_unknown_loss')
    )

    
    ## Create ds_at_mseg ##########################################################
    
    ds_at_po_sel = (
        ds_at_po
        .select('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                'calendar_day', 'process_order_number', 'productid', 
                'logsheet_number', 'bottleneck')
        .drop_duplicates()
    )
    
    ds_at_mseg = in_nr_1_mseg.join(
        ds_at_po_sel, how='inner', on='process_order_number')

    ds_at_mseg_sel = (
        ds_at_mseg
        .filter(col('bwart') == '101')
        .select('enterprise', 'site', 'plant', 'process_cell', 'unit', 
                'calendar_day', 'process_order_number', 'productid', 'matnr', 
                'charg', 'bottleneck', 'logsheet_number')
        .drop_duplicates()
    )
    
    ds_at_mseg_charg = (
        ds_at_mseg_sel
        .join(in_nr_1_mseg.drop('process_order_number','matnr'), 
              how='inner', on=['charg'])
    )
    
    ds_at_mseg_umcha = (
        ds_at_mseg_sel
        .withColumnRenamed('charg', 'umcha')
        .join(in_nr_1_mseg.drop('process_order_number','matnr'), 
              how='inner', on=['umcha'])
    )
    
    print("8")
    ds_at_mseg = (
        ds_at_mseg
        .select([col for col in ds_at_mseg_charg.columns])
        .union(ds_at_mseg_charg)
    )
    print("9")
    ds_at_mseg = (
        ds_at_mseg
        .select([col for col in ds_at_mseg_umcha.columns])
        .union(ds_at_mseg_umcha)
        .drop('insmk')
    )
    
    ds_at_mseg = (
        ds_at_mseg
        .sort('mblnr', 'zeile', 'process_order_number')
        .drop_duplicates(subset=['mblnr', 'zeile', 'process_order_number'])
    )
    
    
    ## Create ds_at_mseg-q1 #######################################################
    
    ds_at_mseg_charg_date = (
        ds_at_mseg_charg
        .select(col('process_order_number'), col('budat_mkpf'))
        .drop_duplicates()
        .sort('process_order_number', 'budat_mkpf')
        .drop_duplicates(subset=['process_order_number'])
        .withColumnRenamed('budat_mkpf', 'first_booking_date')
        .withColumnRenamed('process_order_number' , 'process_order_number')
    )
    
    ds_at_mseg_q1 = (
        ds_at_mseg_charg
        .withColumnRenamed('process_order_number' , 'process_order_number')
        .join(ds_at_mseg_charg_date, how='inner', on='process_order_number')
    )
    
    ds_at_mseg_q1 = ds_at_mseg_q1.filter(
        (col('xauto') != 'X') | col('xauto').isNull())

    ds_at_mseg_q1 = (
        ds_at_mseg_q1
        .withColumn('menge-s', 
                    when(col('shkzg') == 'S', col('menge'))
                    .otherwise(-1 * col('menge')))
        .withColumn('in_q1_time', 
                    datediff(col('budat_mkpf'), col('first_booking_date')) <= 7)
        .withColumn('quantity_prod-erp', 
                    when(col('in_q1_time') 
                         & ((col('bwart') == '101') | (col('bwart') == '102')),
                         col('menge-s'))
                    .otherwise(lit(0.0)))
        .withColumn('quantity_q1_good-erp1', 
                    when(col('in_q1_time') 
                         & ((col('bwart') == '321') | (col('bwart') == '342') 
                            | (col('bwart') == '343')),
                         col('menge'))
                    .otherwise(lit(0.0)))
        .withColumn('quantity_q1_good-erp2', 
                    when(col('in_q1_time') 
                         & ((col('bwart') == '322') | (col('bwart') == '341') 
                            | (col('bwart') == '344')),
                         -1 * col('menge'))
                    .otherwise(lit(0.0)))
        .withColumn('quantity_q1_good-erp3', 
                    when(col('in_q1_time') 
                         & ((col('bwart') == '101') | (col('bwart') == '102'))
                         & ((col('insmk') == 'F') | col('insmk').isNull()),
                         col('menge-s'))
                    .otherwise(lit(0.0)))
        .withColumn('quantity_q1_good-erp', 
                    col('quantity_q1_good-erp1') + col('quantity_q1_good-erp2')
                    + col('quantity_q1_good-erp3'))
        .withColumn('quantity_q1_bad-erp', 
                    col('quantity_prod-erp') - col('quantity_q1_good-erp'))
        .drop('insmk', 'quantity_q1_good-erp1', 'quantity_q1_good-er2', 
              'quantity_q1_good-erp3')
    )


    ## Select columns #########################################################

    ds_at_availability = ds_at_availability.select(
        'shift',
        'category',
        'oee_category',
        'iscosted',
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'event_time',
        'process_order_number',
        'target_prod_rate',
        'running_state',
        'reason',
        'reason_group_desc_2',
        'duration_s',
        'explanation',
        'reason_group_desc_1',
        'calendar_day',
        'target_duration',
        'step_number',
        'step_description',
        'mpc',
        'default_bottleneck',
        'runtime_loss',
        'available',
        'non_impactable',
        'impactable',
        'costed',
        'non-costed',
        'running_potential',
        'availability_loss',
        'unknown_loss',
        'external_loss',
        'maintenance_loss',
        'operations_loss',
        'productid',
        'material_grade',
        'material_color',
        'bottleneck',
        'logsheet_number'
    )
    
    ds_at_mseg = ds_at_mseg.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'calendar_day',
        'process_order_number',
        'productid',
        'logsheet_number',
        'bottleneck',
        'mblnr',
        'aufnr',
        'xauto',
        'zeile',
        'weunb',
        'umlgo',
        'meins',
        'menge',
        'lgort',
        'pbamg',
        'kzzug',
        'kzvbr',
        'kzbew',
        'budat_mkpf',
        'umcha',
        'shkzg',
        'werks',
        'bwart',
        'charg',
        'cputm_mkpf',
        'cpudt_mkpf',
        'umwrk',
        'ummat',
        'erfme',
        'erfmg',
        'matnr',
        'usnam_mkpf',
    )
    
    ds_at_mseg_q1 = ds_at_mseg_q1.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'calendar_day',
        'process_order_number',
        'productid',
        'bottleneck',
        'logsheet_number',
        'mblnr',
        'aufnr',
        'xauto',
        'zeile',
        'weunb',
        'umlgo',
        'meins',
        'menge',
        'lgort',
        'pbamg',
        'kzzug',
        'kzvbr',
        'kzbew',
        'budat_mkpf',
        'umcha',
        'shkzg',
        'werks',
        'bwart',
        'charg',
        'cputm_mkpf',
        'cpudt_mkpf',
        'umwrk',
        'ummat',
        'erfme',
        'erfmg',
        'matnr',
        'usnam_mkpf',
        'first_booking_date',
        'menge-s',
        'in_q1_time',
        'quantity_prod-erp',
        'quantity_q1_good-erp',
        'quantity_q1_bad-erp',
    )

    ds_at_oee = ds_at_oee.select(
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'unit',
        'calendar_day',
        'process_order_number',
        'productid',
        'material_grade',
        'material_color',
        'bottleneck',
        'sum_runtime_loss',
        'sum_available',
        'sum_non_impactable',
        'sum_impactable',
        'sum_running_potential',
        'sum_availability_loss',
        'sum_unknown_loss',
        'sum_external_loss',
        'sum_maintenance_loss',
        'sum_operations_loss',
        'sum_costed',
        'sum_non-costed',
        'sum_capacity',
        'sum_quantity_prod',
        'sum_quantity_ftr',
        'sum_runtime_sec',
        'sum_downtime_sec',
        'sum_quantity_non-ftr',
        'sum_rate_gap'
    )
    
    ds_at_po = ds_at_po.select(
        'process_order_number',
        'productid',
        'unit',
        'actual_start_time',
        'actual_end_time',
        'calendar_day',
        'target_prod_rate',
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'bottleneck',
        'ftr_%',
        'logsheet_number',
        'material_grade',
        'material_color',
        'blocked_erp',
        'quantity_prod_erp',
        'raw_materials_erp',
        'ftr_erp',
        'rework_erp',
        'scrap_erp',
        'obsolete_erp',
        'intra_material_erp',
        'free_for_use_erp',
        'default_values_used',
        'quantity_prod',
        'quantity_required',
        'target_setup_time',
        'target_rate',
        'target_availability',
        'target_quality'
    )

    ds_at_rt_ftr = ds_at_rt_ftr.select(
        'process_order_number',
        'productid',
        'unit',
        'calendar_day',
        'target_prod_rate',
        'enterprise',
        'site',
        'plant',
        'process_cell',
        'bottleneck',
        'ftr_%',
        'logsheet_number',
        'material_grade',
        'material_color',
        'default_bottleneck',
        'downtime_sec',
        'runtime_sec',
        'quantity_prod',
        'quantity_good',
        'quantity_ftr',
        'start_ts',
        'capacity'
    )
    
    ## Append dataframes to list ##############################################

    availability_list.append(ds_at_availability)
    mseg_list.append(ds_at_mseg)
    mseg_q1_list.append(ds_at_mseg_q1)
    oee_list.append(ds_at_oee)
    po_list.append(ds_at_po)
    rt_ftr_list.append(ds_at_rt_ftr)



###############################################################################
## Combine and finalize #######################################################
###############################################################################

## Union all sites ############################################################

availability = reduce(DataFrame.unionAll, availability_list)
mseg = reduce(DataFrame.unionAll, mseg_list)
mseg_q1 = reduce(DataFrame.unionAll, mseg_q1_list)
oee = reduce(DataFrame.unionAll, oee_list)
opn = reduce(DataFrame.unionAll, opn_list)
po = reduce(DataFrame.unionAll, po_list)
rt_ftr = reduce(DataFrame.unionAll, rt_ftr_list)
ve = reduce(DataFrame.unionAll, ve_list)

## Aggregate tables ###########################################################

q1_summary = (
    mseg_q1
    .groupby('process_order_number')
    .agg(sum('quantity_prod-erp').alias('produced'),
         sum('quantity_q1_good-erp').alias('good'),
         sum('quantity_q1_bad-erp').alias('bad'))
    .withColumn('q1',
                when((col('produced') == 0)
                     & (col('good') == 0),
                     lit(0.0))
                .otherwise(least(greatest(col('good') / col('produced'), lit(0.0)), lit(1.0))))
)

po_summary = (
    oee
    .groupby('process_order_number', 'unit')
    .agg(sum('sum_available').alias('sum_available'),
         sum('sum_quantity_ftr').alias('sum_quantity_ftr'))
    .withColumn('po_key', concat(col('process_order_number'), col('unit')))
)

step_summary = (
    availability
    .groupby('process_order_number', 'unit', 'productid', 'step_number', 'step_description')
    .agg(mean('target_duration').alias('step_time_target_min'),
         sum('duration_s').alias('step_duration_min'),
         sum('duration_s').alias('step_duration'),
         mean('target_duration').alias('step_time_target'),
         sum('costed').alias('costed_min'),
         sum('non-costed').alias('non_costed_min'))
    .withColumn('step_time_target_min', col('step_time_target_min') / 60)
    .withColumn('step_duration_min', col('step_duration_min') / 60)
    .withColumn('step_duration', col('step_duration') / 24 / 3600)
    .withColumn('step_time_target', col('step_time_target') / 24 / 3600)
    .withColumn('costed', col('step_time_target') / 60)
    .withColumn('non-costed', col('non_costed_min') / 60)
    .withColumn('po_key', concat(col('process_order_number'), col('unit')))
)

keys = (
    oee
    .select('enterprise', 'site', 'plant', 'process_cell', 'unit', 
            'calendar_day', 'process_order_number', 'bottleneck', 'productid')
    .drop_duplicates()
    .withColumn('key',
                concat(col('enterprise'), col('site'), col('plant'), 
                col('process_cell'), col('unit'), col('calendar_day'), 
                col('process_order_number'), col('bottleneck'), 
                col('productid')))
    .withColumn('calendar_week', concat(year(col('calendar_day')), 
                                        lit(' W'), 
                                        date_format(col('calendar_day'), 'w')))
    .withColumn('calendar_month', date_format(col('calendar_day'), 'MMM yyyy'))
    .withColumn('calendar_year', year(col('calendar_day')))
    .withColumn('indexMonth', 
                date_format(col('calendar_day'), 'yyyyMM').cast('int'))
    .withColumn('weeknum', weekofyear(col('calendar_day')))
)



## Add columns ################################################################

availability = (
    availability
    .withColumn('duration_hrs', col('duration_s') / 3600)
    .withColumn('key',
                concat(col('enterprise'), col('site'), col('plant'), 
                col('process_cell'), col('unit'), col('calendar_day'), 
                col('process_order_number'), col('bottleneck'), 
                col('productid')))
    .withColumn('lost_capacity_ton', col('runtime_loss') * 0.00045359237)
)

mseg = (
    mseg
    .withColumn('entry_ts',to_timestamp(concat_ws(' ', col('cpudt_mkpf').cast('string'), col('cputm_mkpf'))))
    .withColumn('menge_kg', 
                when(col('meins') == 'LB', col('menge') * 0.45359237)
                .otherwise(col('menge')))
    .withColumn('menge_lb', 
                when(col('meins') == 'LB', col('menge'))
                .otherwise(col('menge') / 0.45359237))
    .withColumn('po_key', concat(col('process_order_number'), col('unit')))
)
 
mseg_q1 = (
    mseg_q1
    .withColumn('po_key', concat(col('process_order_number'), col('unit')))
    .withColumn('quantity_q1-erp', col('quantity_q1_good-erp'))
)

oee = (
    oee
    .withColumn('key',
                concat(col('enterprise'), col('site'), col('plant'), 
                col('process_cell'), col('unit'), col('calendar_day'), 
                col('process_order_number'), col('bottleneck'), 
                col('productid')))
    .join(q1_summary.select('process_order_number', 'q1'), 
          how='left', on='process_order_number')
    .withColumn('sum_quantity_non-q1', 
                when(col('q1').isNull(), lit(0.0))
                .otherwise((1 - col('q1')) * col('sum_quantity_prod')))
    .withColumn('sum_quantity_q1', 
                when(col('q1').isNull(),  col('sum_quantity_prod'))
                .otherwise(col('q1') * col('sum_quantity_prod')))
    .drop('q1')
)


opn = (
    opn
    .withColumn('key',
                concat(col('enterprise'), col('site'), col('plant'), 
                col('process_cell'), col('unit'), col('calendar_day'), 
                col('process_order_number'), col('bottleneck'), 
                col('productid')))
)    


po = (
    po
    .withColumn('batch_duration_min', 
                (col('actual_end_time').cast("long") - col('actual_start_time').cast("long")) / 60)
    .withColumn('key',
                concat(col('enterprise'), col('site'), col('plant'), 
                col('process_cell'), col('unit'), col('calendar_day'), 
                col('process_order_number'), col('bottleneck'), 
                col('productid')))                
    .withColumn('po_key', concat(col('process_order_number'), col('unit')))
    .withColumn('quantity_prod_ton', col('quantity_prod') * 0.001)
    .withColumn('target_batch_duration', 
                col('quantity_required') / col('target_prod_rate') * 3600)
)

rt_ftr = (
    rt_ftr
    .withColumn('key',
                concat(col('enterprise'), col('site'), col('plant'), 
                col('process_cell'), col('unit'), col('calendar_day'), 
                col('process_order_number'), col('bottleneck'), 
                col('productid')))  
)

ve = (
    ve
    .withColumn('booked_in_sap', 
                when(col('meins') == 'LB', col('ve_dosedscanned'))
                .otherwise(col('ve_dosedscanned') / 0.45359237))
    .withColumn('key',
                concat(col('enterprise'), col('site'), col('plant'), 
                col('process_cell'), col('unit'), col('calendar_day'), 
                col('process_order_number'), col('bottleneck'), 
                col('productid')))  
)


## Cast columns ###############################################################

availability = (
    availability
    .withColumn('available', col('available').cast('int'))
    .withColumn('non_impactable', col('non_impactable').cast('int'))
    .withColumn('impactable', col('impactable').cast('int'))
)

mseg = (
    mseg
    .withColumn('entry_ts', to_timestamp(col('entry_ts'), 'yyyy-MM-dd HH:mm:ss'))
    .withColumn('menge', col('menge').cast('float'))
    .withColumn('menge_kg', col('menge_kg').cast('float'))
    .withColumn('menge_lb', col('menge_lb').cast('float'))
    .withColumn('erfmg', col('erfmg').cast('float'))
    .withColumn('budat_mkpf', to_date(col('budat_mkpf'), 'yyyy-MM-dd'))
    .withColumn('cpudt_mkpf', to_date(col('cpudt_mkpf'), 'yyyy-MM-dd'))
)

mseg_q1 = (
    mseg_q1
    .withColumn('menge', col('menge').cast('float'))
    .withColumn('menge-s', col('menge-s').cast('float'))
    .withColumn('erfmg', col('erfmg').cast('float'))
    .withColumn('pbamg', col('pbamg').cast('int'))
    .withColumn('quantity_prod-erp', col('quantity_prod-erp').cast('float'))
    .withColumn('budat_mkpf', to_date(col('budat_mkpf'), 'yyyy-MM-dd'))
    .withColumn('cpudt_mkpf', to_date(col('cpudt_mkpf'), 'yyyy-MM-dd'))
)

oee = (
    oee
    .withColumn('sum_impactable', col('sum_impactable').cast('int'))
    .withColumn('sum_runtime_sec', col('sum_runtime_sec').cast('int'))
    .withColumn('sum_downtime_sec', col('sum_downtime_sec').cast('int'))
    .withColumn('sum_quantity_non-ftr', col('sum_quantity_non-ftr').cast('int'))
)

po = (
    po
    .withColumn('quantity_prod', col('quantity_prod').cast('int'))
    .withColumn('ftr_erp', col('ftr_erp').cast('int'))
    .withColumn('raw_materials_erp', col('raw_materials_erp').cast('int'))
    .withColumn('blocked_erp', col('blocked_erp').cast('int'))
    .withColumn('rework_erp', col('rework_erp').cast('int'))
    .withColumn('scrap_erp', col('scrap_erp').cast('int'))
    .withColumn('obsolete_erp', col('obsolete_erp').cast('int'))
    .withColumn('intra_material_erp', col('intra_material_erp').cast('int'))
    .withColumn('free_for_use_erp', col('free_for_use_erp').cast('int'))
    .withColumn('ftr_%', col('ftr_%').cast('int'))
)

rt_ftr = (
    rt_ftr
    .withColumn('quantity_good', col('quantity_good').cast('int'))
    .withColumn('ftr_%', col('ftr_%').cast('int'))
)
ve = (
    ve
    .withColumn('booked_in_sap', col('booked_in_sap').cast('float'))
)


## Rename columns #############################################################

opn = (
    opn
    .withColumnRenamed('eventts', 'event_timestamp')
)

ve = (
    ve
    .withColumnRenamed('ve_machine', 'unit/feedpoint')
    .withColumnRenamed('ve_materialdescription', 'material_description')
    .withColumnRenamed('ve_bom', 'bom_quantity')
    .withColumnRenamed('ve_dosedfeeders', 'dcs_measurement')
)


## Select columns #############################################################

availability = availability.select(
    'oee_category',
    'enterprise',
    'site',
    'plant',
    'process_cell',
    'unit',
    'event_time',
    'process_order_number',
    'target_prod_rate',
    'shift',
    'running_state',
    'reason',
    'reason_group_desc_2',
    'duration_s',
    'explanation',
    'category',
    'reason_group_desc_1',
    'calendar_day',
    'mpc',
    'runtime_loss',
    'available',
    'non_impactable',
    'impactable',
    'running_potential',
    'availability_loss',
    'unknown_loss',
    'external_loss',
    'maintenance_loss',
    'operations_loss',
    'productid',
    'material_grade',
    'material_color',
    'key',
    'target_duration',
    'bottleneck',
    'logsheet_number',
    'iscosted',
    'costed',
    'lost_capacity_ton',
    'non-costed',
    'step_number',
    'duration_hrs',
    'default_bottleneck',
    'step_description'
)

mseg = mseg.select(
    'enterprise',
    'site',
    'plant',
    'process_cell',
    'process_order_number',
    'productid',
    'bwart',
    'matnr',
    'werks',
    'charg',
    'menge',
    'meins',
    'erfmg',
    'erfme',
    'aufnr',
    'pbamg',
    'kzbew',
    'kzvbr',
    'kzzug',
    'weunb',
    'po_key',
    'unit',
    'budat_mkpf',
    'cpudt_mkpf',
    'cputm_mkpf',
    'usnam_mkpf',
    'entry_ts',
    'xauto',
    'shkzg',
    'umcha',
    'bottleneck',
    'logsheet_number',
    'calendar_day',
    'lgort',
    'ummat',
    'umwrk',
    'umlgo',
    'mblnr',
    'zeile',
    'menge_lb',
    'menge_kg'
)

mseg_q1 = mseg_q1.select(
    'enterprise',
    'site',
    'plant',
    'process_cell',
    'unit',
    'calendar_day',
    'process_order_number',
    'productid',
    'bottleneck',
    'logsheet_number',
    'mblnr',
    'zeile',
    'bwart',
    'xauto',
    'matnr',
    'werks',
    'lgort',
    'charg',
    'shkzg',
    'menge',
    'meins',
    'erfmg',
    'erfme',
    'aufnr',
    'pbamg',
    'ummat',
    'umwrk',
    'umlgo',
    'umcha',
    'kzbew',
    'kzvbr',
    'kzzug',
    'weunb',
    'budat_mkpf',
    'cpudt_mkpf',
    'cputm_mkpf',
    'usnam_mkpf',
    'first_booking_date',
    'menge-s',
    'in_q1_time',
    'quantity_prod-erp',
    'quantity_q1_good-erp',
    'quantity_q1_bad-erp',
    'po_key',
    'quantity_q1-erp'
)

oee = oee.select(
    'enterprise',
    'site',
    'plant',
    'process_cell',
    'unit',
    'calendar_day',
    'process_order_number',
    'productid',
    'material_grade',
    'material_color',
    'sum_runtime_loss',
    'sum_available',
    'sum_non_impactable',
    'sum_impactable',
    'sum_running_potential',
    'sum_availability_loss',
    'sum_unknown_loss',
    'sum_external_loss',
    'sum_maintenance_loss',
    'sum_operations_loss',
    'sum_capacity',
    'sum_quantity_prod',
    'sum_quantity_ftr',
    'sum_runtime_sec',
    'sum_downtime_sec',
    'sum_quantity_non-ftr',
    'sum_rate_gap',
    'key',
    'bottleneck',
    'sum_costed',
    'sum_non-costed',
    'sum_quantity_non-q1',
    'sum_quantity_q1'
)

opn = opn.select(
    'enterprise',
    'site',
    'plant',
    'process_cell',
    'unit',
    'kop',
    'hourstartts',
    'eventtype',
    'process_order_number',
    'interval',
    'data_refresh_dt',
    'calendar_day',
    'productid',
    'material_grade',
    'material_color',
    'bottleneck',
    'logsheet_number',
    'key',
    'event_timestamp',
    'kop_activated',
    'alarm_duration',
    'alarm_text'
)

po = po.select(
    'enterprise',
    'site',
    'plant',
    'process_cell',
    'unit',
    'process_order_number',
    'productid',
    'quantity_prod',
    'target_prod_rate',
    'actual_start_time',
    'actual_end_time',
    'material_grade',
    'material_color',
    'quantity_required',
    'quantity_prod_erp',
    'ftr_erp',
    'raw_materials_erp',
    'blocked_erp',
    'rework_erp',
    'scrap_erp',
    'obsolete_erp',
    'intra_material_erp',
    'free_for_use_erp',
    'target_rate',
    'target_availability',
    'target_quality',
    'target_setup_time',
    'default_values_used',
    'calendar_day',
    'bottleneck',
    'logsheet_number',
    'key',
    'batch_duration_min',
    'target_batch_duration',
    'po_key',
    'quantity_prod_ton',
    'ftr_%',
)

rt_ftr = rt_ftr.select(
    'productid',
    'material_grade',
    'material_color',
    'enterprise',
    'site',
    'plant',
    'process_cell',
    'unit',
    'process_order_number',
    'capacity',
    'quantity_prod',
    'quantity_good',
    'target_prod_rate',
    'runtime_sec',
    'downtime_sec',
    'calendar_day',
    'quantity_ftr',
    'key',
    'bottleneck',
    'logsheet_number',
    'default_bottleneck',
    'ftr_%',
    'start_ts'
)

ve = ve.select(
    'enterprise',
    'site',
    'plant',
    'process_cell',
    'unit',
    'process_order_number',
    'materialnumber',
    've_quantityproduced',
    've_dosedscanned',
    've_check',
    've_explanation',
    've_remark',
    'data_refresh_dt',
    'productid',
    'bottleneck',
    'logsheet_number',
    'key',
    'calendar_day',
    'bwart',
    'meins',
    'booked_in_sap',
    'unit/feedpoint',
    'material_description',
    'bom_quantity',
    'dcs_measurement'
)

## Write dataframes to parquet ################################################

availability.write.mode("overwrite").parquet(
    '{}/availability/'.format(destination))

mseg.write.mode("overwrite").parquet(
    '{}/mseg/'.format(destination))

mseg_q1.write.mode("overwrite").parquet(
    '{}/mseg_q1/'.format(destination))

oee.write.mode("overwrite").parquet(
    '{}/oee/'.format(destination))

opn.write.mode("overwrite").parquet(
    '{}/opn/'.format(destination))

po.write.mode("overwrite").parquet(
    '{}/po/'.format(destination))
    
rt_ftr.write.mode("overwrite").parquet(
    '{}/rt_ftr/'.format(destination))
    
ve.write.mode("overwrite").parquet(
    '{}/ve/'.format(destination))
 
q1_summary.write.mode("overwrite").parquet(
    '{}/q1_summary/'.format(destination))

po_summary.write.mode("overwrite").parquet(
    '{}/po_summary/'.format(destination))

step_summary.write.mode("overwrite").parquet(
    '{}/step_summary/'.format(destination))

keys.write.mode("overwrite").parquet(
    '{}/keys/'.format(destination))


job.commit()
