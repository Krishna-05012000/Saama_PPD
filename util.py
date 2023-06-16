import pandas as pd
import traceback
from functools import reduce
def udf_generate_preds(domain_dict, query_text):
    import pandas as pd
    import traceback
    from functools import reduce

    """
    query_text - Query text corresponding to the Rule/DQ.
    args - any number of arguments(attribute name) to be included in the query string.
    """
    # domain_dict
    # data_load = {
    #     "data":{"MH":MH,"EX":EX},
    #     "filters": {
    #         "form_id": {"MH": [["mh"], 'isin'], "EX": [["ec_injection"], 'isin']},
    #         "visit_id": {"MH": [["scrn"], 'isin'], "EX": [["visit1"], 'isin']},
    #         "filter1" : ["EXSTDTC", "MHSTDTC", "<", "compare"],
    #         "filter2": ["MHSOC", ["surgical and medical procedures"], "isin"],
    #         "filter3": ["MHTERM", ["neuropathy"], "pwmatch"]
    #     },
    #     "filter_str": "filter1 & filter2 & filter3", -- Optional
    #     "convert_to_date": {
    #         "MH": ["MHSTDTC"],
    #         "EX": ["EXSTDTC"]
    #     },
    #     "query_params" : {
    #                       "MH" : ['MHSTDTC', 'MHSOC'],
    #                       "EX" : ['EXSTDTC']
    #                       }  
    # }

    df_list = list(domain_dict['data'].values())
    if 'convert_to_date' in domain_dict:
        for dom, variable in domain_dict['convert_to_date'].items():
            for var in variable:
                try:
                    if dom in domain_dict['data'].keys():
                        df = domain_dict['data'][dom]
                        df[var].apply(udf_convert_to_date).apply(pd.to_datetime)
                        df = df.dropna(subset=[var])

                except Exception as e:
                    print(f'Error {e}',dom)

    if type(df_list) is pd.DataFrame:
    #1. Filter Primary domain
        filtered_primary_df = udf_generic_filter(df_list, domain_dict["filters"], domain_dict["filter_str"])

    elif type(df_list) is list:
        domain_vs_filter = {}
        udf_generic_filters = {}
        columns_list = ['form_index', 'modif_dts', 'ck_event_id']
        if "add_payload" in domain_dict:
            columns_list.extend(*list(domain_dict["add_payload"].values()))
        for filter_, value in domain_dict['filters'].items():
            if type(value) is dict:
                for key_, value_ in value.items():
                    if key_ not in domain_vs_filter:
                        domain_vs_filter[key_] = {}
                    value_u = [filter_]
                    value_u.extend(value_)
                    domain_vs_filter[key_].update({filter_ : value_u})
                continue
            udf_generic_filters.update({filter_ : value})
            if len(value) > 3 and value[3] == 'compare':
                columns_list.append(value[1])
            columns_list.append(value[0])

        new_df_list = []
        var_dict = {}
        for index in range(len(df_list)):
            df = df_list[index].copy()
            domain = df['domain'].values[0]
            if domain in domain_vs_filter:
                filter_str = ' & '.join(domain_vs_filter[domain].keys())
                df = udf_generic_filter(df, domain_vs_filter[domain], filter_str)
            df_columns = [column for column in df.columns.tolist() if column in columns_list]
            df = df[df_columns]
            df = df.add_suffix('_' + str(index))
            var_dict.update({column : column + '_' + str(index) for column in df_columns})
            df['key'] = 1
            new_df_list.append(df)
            index += 1
        
        if "filter_str" not in domain_dict:
            filter_str = " & ".join(udf_generic_filters.keys())
        else:
            filter_str = domain_dict["filter_str"]

        merged_df = reduce(lambda left, right: pd.merge(left, right, how='outer'), new_df_list)
        filtered_primary_df = udf_generic_filter(merged_df, udf_generic_filters, filter_str, var_dict)

    #3. TODO link primary domain with secondary domains and get the final resluts

    if len(filtered_primary_df) <= 0:
        return []
    
    payload_records=[]
    query_params = {}
    if "query_params" in domain_dict:
        for domain in domain_dict['query_params']:
            params = domain_dict['query_params'][domain]
            for param in params:
                if param in var_dict:
                    param = var_dict[param]
                query_params.update( {param : filtered_primary_df[param].values.tolist()})
    
    col_list=[ col for col in filtered_primary_df.columns.tolist() if 'ck_event_id' in col ]
    for i in range(len(filtered_primary_df)):
        query_text_formatted = query_text
        df_record = filtered_primary_df.iloc[[i]]
        key = filtered_primary_df.iloc[i]['ck_event_id_0']
        value = []
        for x in range(1,len(col_list)):
            value.append(df_record['ck_event_id_'+str(x)].values[0])
        if query_params:
            try:
                query_params_i = [value[i] for value in query_params.values()]
                query_text_formatted = query_text % tuple(query_params_i)
            except:
                print()
        payload = {}
        payload_extras = domain_dict["add_payload"]
        if "add_payload" in domain_dict:
            for domain in payload_extras:
                vars = payload_extras[domain]
                suffix = list(payload_extras.keys()).index(domain)
                if suffix != -1:
                    payload.update({var : df_record[var + "_" + str(suffix)].values[0] for var in vars})
        payload.update({
                "query_text": query_text_formatted,
                "form_index": str(df_record['form_index_0'].values[0]),
                "modif_dts": str(pd.to_datetime(df_record['modif_dts_0'].values[0])),
                "stg_ck_event_id": int(key)
            })
        if value:
            payload["relational_ck_event_ids"] = value
        payload_records.append(payload)              
    return payload_records



def udf_generic_filter(df : pd.DataFrame, filter_dict : dict, condition : str, var_dict = {}):
    """
    df - dataframe to be filtered 
    filter_dict - dictionary containing attribute name and value to be compared
    --Types Allowed in comparison
        isin, isnotin - exact match of str
        pmatch, npmatch - partial match of str
        isnull, notnull - Null check
    condition - condition string combining all the conditions
    """
    con_str = ""
    try:
        
        if df.empty:
            return pd.DataFrame()

        key_dict = {}
        for filter_name, value in filter_dict.items():

            updated_var_name = value[0]
            if updated_var_name in var_dict:
                updated_var_name = var_dict[updated_var_name]
            value_ = value[1]
            condition_value = None
            if value_ in ["isnull", "notnull"]:
                condition_value = "(df['" + updated_var_name +"']." + value_ + "())"
            else:
                if value[2] in ["isin", "isnotin"]:
                    condition_value = "(df['" + updated_var_name +"'].str.strip().str.lower().isin(" + str(value_) + "))"
                    if value[2] == "isnotin":
                        condition_value = condition_value[0] + "~" + condition_value[1:]
                elif value[2] in ["pmatch", "npmatch"]:
                    condition_value = "(df['" + updated_var_name +"'].str.lower().str.contains('|'.join(" + str(value_) + "), na=False))"
                    if value[2] == "npmatch":
                        condition_value = condition_value = condition_value[0] + "~" + condition_value[1:]
                elif value[2] in ["pwmatch"]:
                     condition_value = "(df['" + updated_var_name +"'].str.lower().str.strip().apply(udf_partial_word_match, term_list=" + str(value[1]) + "))"
                else:
                    value_c = value_
                    isDate = True if value_c.endswith('DTC') or value_c.endswith('DAT') else False
                    if value_c in var_dict:
                        value_c = var_dict[value_c]
                    if isDate:
                        condition_value = "(df['" + updated_var_name +"'] " + value[2] + " df['" + value_c + "'])"
                    else:
                        condition_value = "(df['" + updated_var_name +"'].str.lower().str.strip() " + value[2] + " df['" + value_c + "'].str.lower().str.strip())"
            
            key_dict[filter_name] = condition_value

        con_str = condition
        for filter_name in key_dict:
            con_str = con_str.replace(filter_name, key_dict[filter_name])

        return eval("df["+ con_str + "]")

    except:
        traceback.print_exc()

        return pd.DataFrame()
