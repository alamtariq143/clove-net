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
       
    # sap data 
    in_nr_1_mseg = spark.read.option("header", True).csv(
        '{}/{}'.format(site_source, 'drf_sap.dbo.in_nr_1_mseg.csv'))  
    
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
    
    # rename columns
    reporting_cat_md = reporting_cat_md.withColumnRenamed(
        'reporting_category', 'oee_category')
    s95_md = s95_md.withColumnRenamed('unit_procedureunit', 'unit')
    
    
    ## Drop empty rows ############################################################

    procedure = procedure.filter(~col('procedurearea').isNull())
    explanation = explanation.filter(~col('explanationcost').isNull())
    
    
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
    

    # fill na
    fill_0 = ['yieldid', 'yieldactual', 'yieldlcost', 'yieldloss', 'yieldtarget', 
              'explanationid', 'explanationinstance', 'explanationcost', 
              'explanationloss']
    merged = merge_7.na.fill(value=0, subset=fill_0)
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
    

    ## Aggregate data 1 ###########################################################
    
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
    
    aggregated = (
        enriched
        .groupBy(groupby_1_cols)
        .agg(sum('explanationcost').alias('sum_explanationcost'),
             sum('explanationloss').alias('sum_explanationloss'),
             sum('lossexternal').alias('sum_lossexternal'),
             sum('lossoperations').alias('sum_lossoperations'),
             sum('lossmaintenance').alias('sum_lossmaintenance'))
    )
    

    ## Add columns to aggregated 1 ################################################
    
    aggregated = aggregated.withColumn('durationidle', col('sum_lossexternal'))
    
    aggregated = aggregated.withColumn(
        'durationrunning',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(least(col('durationactual') - col('sum_lossexternal'), 
                         col('durationtarget')))
    )
    
    aggregated = aggregated.withColumn(
        'durationextratime',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(greatest(col('durationactual') 
                            - col('durationtarget') 
                            - col('sum_lossexternal'),
                            lit(0)))
    ) 
    
    aggregated = aggregated.withColumn(
        'durationrunningtarget',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(col('durationtarget'))
    )
    
    aggregated = aggregated.withColumn(
        'durationstopped',
        when(col('operationtype') == 'Stopped', 
             greatest(col('durationactual') - col('sum_lossexternal'), lit(0)))
        .otherwise(lit(0))
    )
    
    
    ## Aggregate data 2 ###########################################################
    
    groupby_2_cols = [
        'enterprise', 'site', 'plant', 'process_cell', 'bottleneck', 'mpc', 
        'procedureid', 'procedurearea', 'productid',
        'process_order_number', 'procedureproduct_group', 
        'procedurerecipe_code', 'procedurestart', 'procedureend', 
        'procedurecalendar_day', 'yieldtarget', 'yieldactual', 'unit_procedureid', 
        'unit_procedurename', 'unit', 'unit_procedurestart', 'unit_procedureend', 
        'unit_procedurecalendar_day', 
    ]
    
    aggregated = (
        aggregated
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
    

    ## Add columns to aggregated 2 ################################################
    
    aggregated = aggregated.withColumn(
        'sum_durationrunningtotal',
        col('sum_durationrunning') + col('sum_durationextratime')
    )
    
    aggregated = aggregated.withColumn(
        'target_prod_rate',
        when((col('yieldtarget') == 0) | (col('yieldtarget').isNull()), 
             1 / col('mpc'))
        .otherwise(col('sum_durationrunningtarget') / 60 / col('yieldtarget'))
    )
    
    aggregated = aggregated.withColumn(
        'target_prod_rate_po',
        when((col('yieldtarget') == 0) | (col('yieldtarget').isNull()), col('mpc'))
        .otherwise(col('yieldtarget') / col('sum_durationrunningtarget') * 60)
    )
    
    aggregated = aggregated.withColumn('ftr_%', lit(100.0))
    aggregated = aggregated.withColumn('logsheet_number', lit(None).cast('string'))
    aggregated = aggregated.withColumn('material_grade', lit(None).cast('string'))
    aggregated = aggregated.withColumn('material_color', lit(None).cast('string'))
    aggregated = aggregated.withColumn('quantity_prod', col('yieldactual'))
    aggregated = aggregated.withColumn(
        'default_bottleneck', lit(None).cast('boolean'))

#-------------------------------------------------------------------------------
for site in aspentech_sites_unapproved:
		    
    unapproved_site_source = '{}/{}/{}/{}'.format(source, 'aspentech', site, 'unapproved')
    
    ## Adding the lines below to skip the folder which doensn't have unapproved subfolder
    response_check = client.list_objects_v2(Bucket= bucket_name, Prefix = unapproved_site_source)
    
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
        '{}/{}'.format(site_source, 'drf_sap.dbo.in_nr_1_mseg.csv'))  
    
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
	
	# columns to snake_case unapproved
	unapproved_duration = to_snake_case(unapproved_duration)
    unapproved_explanation = to_snake_case(unapproved_explanation)
    unapproved_operation = to_snake_case(unapproved_operation)
    unapproved_operation_duration = to_snake_case(unapproved_operation_duration)
    unapproved_procedure = to_snake_case(unapproved_procedure)
    unapproved_unit_procedure = to_snake_case(unapproved_unit_procedure)
    unapproved_yield_ = to_snake_case(unapproved_yield_)
	operations_md = to_snake_case(operations_md)
    reporting_cat_md = to_snake_case(reporting_cat_md)
    s95_md = to_snake_case(s95_md)
    in_nr_1_mseg = to_snake_case(in_nr_1_mseg)
    
    
	unapproved_prefix_yield = ['id', 'actual', 'cost', 'loss', 'target']
    for c in unapproved_prefix_yield:
         unapproved_yield_ = unapproved_yield_.withColumnRenamed(c, "yield"+c)
    
    for c in unapproved_procedure.columns:
         unapproved_procedure = unapproved_procedure.withColumnRenamed(c, "procedure"+c)
         
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
        
    prefix_reporting_cat_md = ['category', 'subcategory', 'group']
    for c in prefix_reporting_cat_md:
        reporting_cat_md = reporting_cat_md.withColumnRenamed(c, "explanation"+c)
    
    # rename columns
    reporting_cat_md = reporting_cat_md.withColumnRenamed(
        'reporting_category', 'oee_category')
    s95_md = s95_md.withColumnRenamed('unit_procedureunit', 'unit')
	
	## Drop empty rows ############################################################

    unapproved_procedure = unapproved_procedure.filter(~col('procedurearea').isNull())
    unapproved_explanation = unapproved_explanation.filter(~col('explanationcost').isNull())

	## Cast columns ###############################################################
    
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
    unapproved_fill_0 = ['yieldid', 'yieldactual', 'yieldlcost', 'yieldloss', 'yieldtarget', 
              'explanationid', 'explanationinstance', 'explanationcost', 
              'explanationloss']
    unapproved_merged = unapproved_merge_7.na.fill(value=0, subset=unapproved_fill_0)
	
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
	

	
	## Unapproved Aggregate data 1 ###########################################################
    
    unapproved_groupby_cols = [
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
    
    unapproved_aggregated = (
        unapproved_enriched
        .groupBy(unapproved_groupby_1_cols)
        .agg(sum('explanationcost').alias('sum_explanationcost'),
             sum('explanationloss').alias('sum_explanationloss'),
             sum('lossexternal').alias('sum_lossexternal'),
             sum('lossoperations').alias('sum_lossoperations'),
             sum('lossmaintenance').alias('sum_lossmaintenance'))
    )
	
	
	## Add columns to Unapproved aggregated 1 ################################################
    
    unapproved_aggregated = unapproved_aggregated.withColumn('durationidle', col('sum_lossexternal'))
    
    unapproved_aggregated = unapproved_aggregated.withColumn(
        'durationrunning',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(least(col('durationactual') - col('sum_lossexternal'), 
                         col('durationtarget')))
    )
    
    unapproved_aggregated = unapproved_aggregated.withColumn(
        'durationextratime',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(greatest(col('durationactual') 
                            - col('durationtarget') 
                            - col('sum_lossexternal'),
                            lit(0)))
    ) 
    
    unapproved_aggregated = unapproved_aggregated.withColumn(
        'durationrunningtarget',
        when(col('operationtype') == 'Stopped', lit(0))
        .otherwise(col('durationtarget'))
    )
    
    unapproved_aggregated = unapproved_aggregated.withColumn(
        'durationstopped',
        when(col('operationtype') == 'Stopped', 
             greatest(col('durationactual') - col('sum_lossexternal'), lit(0)))
        .otherwise(lit(0))
    )
	
	#UNION
	union_agg_1 = aggregated.union(unapproved_aggregated)
	
	## Unapproved Aggregate data 2 ###########################################################
    
    unapproved_groupby_cols = [
        'enterprise', 'site', 'plant', 'process_cell', 'bottleneck', 'mpc', 
        'procedureid', 'procedurearea', 'productid',
        'process_order_number', 'procedureproduct_group', 
        'procedurerecipe_code', 'procedurestart', 'procedureend', 
        'procedurecalendar_day', 'yieldtarget', 'yieldactual', 'unit_procedureid', 
        'unit_procedurename', 'unit', 'unit_procedurestart', 'unit_procedureend', 
        'unit_procedurecalendar_day', 
    ]
    
    unapproved_aggregated = (
        unapproved_aggregated
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
		
	## Add columns to unapproved_aggregated ################################################
    
    unapproved_aggregated = unapproved_aggregated.withColumn(
        'sum_durationrunningtotal',
        col('sum_durationrunning') + col('sum_durationextratime')
    )
    
    unapproved_aggregated = unapproved_aggregated.withColumn(
        'target_prod_rate',
        when((col('yieldtarget') == 0) | (col('yieldtarget').isNull()), 
             1 / col('mpc'))
        .otherwise(col('sum_durationrunningtarget') / 60 / col('yieldtarget'))
    )
    
    unapproved_aggregated = unapproved_aggregated.withColumn(
        'target_prod_rate_po',
        when((col('yieldtarget') == 0) | (col('yieldtarget').isNull()), col('mpc'))
        .otherwise(col('yieldtarget') / col('sum_durationrunningtarget') * 60)
    )
    
    unapproved_aggregated = unapproved_aggregated.withColumn('ftr_%', lit(100.0))
    unapproved_aggregated = unapproved_aggregated.withColumn('logsheet_number', lit(None).cast('string'))
    unapproved_aggregated = unapproved_aggregated.withColumn('material_grade', lit(None).cast('string'))
    unapproved_aggregated = unapproved_aggregated.withColumn('material_color', lit(None).cast('string'))
    unapproved_aggregated = unapproved_aggregated.withColumn('quantity_prod', col('yieldactual'))
    unapproved_aggregated = unapproved_aggregated.withColumn(
        'default_bottleneck', lit(None).cast('boolean'))
		
	#UNION
	
	union_agg_2 = aggregated.union(unapproved_aggregated)
    
    #####UNION OF BOTH union_agg_1 & union_agg_2
    
    #---------------------------------------------------