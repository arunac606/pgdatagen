import pandas as pd
from custlib import autogen_value


def getdistinctcolumns(data):
    temp_df = pd.DataFrame(data, columns=['table_name', 'column_name', 'position', 'maximum_length', 'data_type'])
    distinct_df = temp_df.filter(['column_name', 'maximum_length', 'data_type'], axis=1)
    distinct_df.drop_duplicates(keep='last', inplace=True)
    distinct_df = distinct_df.reset_index(drop=True)
    distinct_df = distinct_df.fillna(0)
    distinct_df['maximum_length'] = distinct_df['maximum_length'].astype(int)
    distinct_df['value'] = '0'
    return distinct_df


def setdata(data):
    data['value'] = data.apply(autogen_value, axis=1)
    data['columnname_maxlength'] = data['column_name'] + '~' + data['maximum_length'].astype(str)
    return data


def datagen(data, schema, n):
    df = pd.DataFrame(data, columns=['table_name', 'column_name', 'position', 'maximum_length', 'data_type'])
    df.fillna(0)
    df['maximum_length'] = df['maximum_length'].astype(int)
    no_dup_data = getdistinctcolumns(df)
    cdata = setdata(no_dup_data)
    cdict = dict(zip(cdata['columnname_maxlength'], cdata['value']))
    xml_filename = "./xml/SampleData{}.xml".format(n)
    with open(xml_filename, "w", encoding="utf-8") as xfp:
        for table, data1 in df.groupby(df['table_name']):
            data1 = data1.sort_values('position', ascending=True)
            l = data1.values.tolist()
            xfp.write("    <" + l[0][0].upper())
            for line in l:
                cval = str(cdict.get(line[1] + '~' + str(line[3]), line[1]))
                cval = cval.replace("'", "&apos;")
                xfp.write(" " + line[1] + "='" + cval + "'")
            xfp.write(" />\n")
    xfp.close

    sql_filename = "./sql/SampleData{}.sql".format(n)
    with open(sql_filename, "w", encoding="utf-8") as sfp:
        for table, data1 in df.groupby(df['table_name']):
            data1 = data1.sort_values('position', ascending=True)
            l = data1.values.tolist()
            first_part = ""
            second_part = ""
            first_part = "INSERT INTO " + schema + "." + l[0][0].upper() + " ("
            first1 = True
            for line in l:
                if first1:
                    first_part = first_part + line[1]
                    first1 = False
                else:
                    first_part = first_part + ", " + line[1]

            first2 = True
            for line in l:
                cval = str(cdict.get(line[1] + '~' + str(line[3]), line[1]))
                cval = cval.replace("'", "&apos;")
                if first2:
                    second_part = "'" + cval + "'"
                    first2 = False
                else:
                    second_part = second_part + ",'" + cval + "'"
            sfp.write(first_part + ") VALUES (" + second_part)
            sfp.write(');\n')
    sfp.close()


if __name__ == '__main__':
    datagen()
