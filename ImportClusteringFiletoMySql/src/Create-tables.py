import pandas as pd
import time
import pymysql
import xlrd
import openpyxl
from openpyxl.workbook import Workbook

def exert_prj_id():
    prj_ids = []
    with open('./data/PRIDE_CLUSTER', 'r') as textfile:
        while True:
            lines = textfile.readline()
            if not lines:
                break
            line = lines.strip("\n")
            prj_ids.append(line)
    textfile.close()
    return prj_ids

# 执行语句
def db_excute(sql):
    # local_infile = 1 执行load data infile
    db = pymysql.connect(host="localhost",user='root',password='123456',port=3306,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    #db = pymysql.connect(host="localhost",user='root',password='root123123',port=3309,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

    cursor = db.cursor()
    try:
        cursor.execute(sql)
        db.commit()
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

def drop_table(cluster_table_name):
    db = pymysql.connect(host="localhost",user='root',password='123456',port=3306,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

    cursor = db.cursor()
    try:
        prj_ids = exert_prj_id()
        for id in prj_ids:
            print(id)
            prj_id = "T_CLUSTER_SPEC_" + id
            sql = "drop table "+ prj_id +";"
            cursor.execute(sql)
        sql1 = "drop table " + cluster_table_name + ";"
        sql2 = "drop table " + cluster_table_name + "_projects;"
        cursor.execute(sql1)
        cursor.execute(sql2)
        db.commit()
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

def drop_cluster_table(cluster_table_name):
    sql = "drop table " + cluster_table_name +";"
    sql2 = "drop table "+ cluster_table_name+ "_projects;"

def Truncate_spec_tables(cluster_table_name):
    db = pymysql.connect(host="localhost",user='root',password='123456',port=3306,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

    cursor = db.cursor()
    try:
        prj_ids = exert_prj_id()
        for id in prj_ids:
            print(id)
            prj_id = "T_CLUSTER_SPEC_" + id
            truncate_spec_sql = "truncate" + prj_id + ";"
            cursor.execute(truncate_spec_sql)
        truncate_cluster_sql = "truncate " + cluster_table_name + ";"
        cursor.execute(truncate_cluster_sql)
        db.commit()
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

def Create_prj_taxids_name_T():
    sql = "drop table prj_taxids_species_01;"
    db_excute(sql)
    print("Finish delete table, the create the new one!")
    create_tabel = "create table PRJ_TAXIDS_SPECIES_02 (id int(15) NOT NULL AUTO_INCREMENT,prj_id varchar(15) not null,tax_id varchar(500),species_name varchar(500), primary key(id))ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
    db_excute(create_tabel)

def Insert():
    db = pymysql.connect(host="localhost",user='root',password='123456',port=3306,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    #db = pymysql.connect(host='localhost',user='root',password='123456',port=3306,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()
    prj_list = []
    taxid_list = []
    species_name_list = []
    col = ['prj_id','tax_id','species_name']
    try:
        df = pd.read_csv('./data/prj_taxid_name_04.csv',keep_default_na=False)
        DataSet = df[["prj_id", "tax_id", "species_name"]]
        tups = [tuple(x) for x in DataSet.values]

        insert_sql = "INSERT INTO PRJ_TAXIDS_SPECIES_02 (prj_id,tax_id,species_name) values(%s,%s,%s);"
        cursor.executemany(insert_sql, tups)
        db.commit()

    except pymysql.Error as e:
        print(e)
    finally:
        db.close()
'''
        for i in range(len(tups)):
            tup = tups[i]
            prj = tup[0]
            prj_list.append(prj)
            taxid = tup[1]
            taxid_list.append(taxid)
            species = tup[-1]
            species_name_list.append(species)

        dataframe = pd.DataFrame({'prj_id':prj_list,'tax_id':taxid_list,'species_name':species_name_list},columns=col)
        dataSet = dataframe[['prj_id','tax_id','species_name']]
        tuples = [tuple(x) for x in dataSet.values]
'''

def Create_Cluster_T(cluster_table_name):
    create_cluster_t = "CREATE TABLE " + cluster_table_name + "(id int(15) NOT NULL AUTO_INCREMENT,cluster_id varchar(100) COLLATE utf8_bin NOT NULL,cluster_ratio float ,n_spec int(10) ,n_id int (10) ,n_unid int(10) ," + \
                    "sequences_ratios text NOT NULL ,sequences_mods text NOT NULL ,spectra_titles MEDIUMTEXT NOT NULL,consensus_mz text NOT NULL,consensus_intens text NOT NULL,conf_sc int(10) NOT NULL,PRIMARY KEY (id), INDEX(cluster_id))ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
    db_excute(create_cluster_t)
    create_projects_t = "CREATE TABLE " + cluster_table_name + "_projects (id int(10) NOT NULL AUTO_INCREMENT,project_id varchar(10) COLLATE utf8_bin NOT NULL,PRIMARY KEY (id))ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
    db_excute(create_projects_t)

def Create_SPEC_T(cluster_table_name):
    prj_ids = exert_prj_id()
    for id in prj_ids:
        print(id)
        prj_id = "T_CLUSTER_SPEC_"+id
        create_spec_t = "CREATE TABLE `"+prj_id+"` (id int(15) NOT NULL AUTO_INCREMENT,spectrum_title varchar(200) COLLATE utf8_bin NOT NULL,charge float NOT NULL,precursor_mz float NOT NULL,taxids varchar(100) NOT NULL,is_spec_identified TINYINT(1) NOT NULL,id_sequences text NOT NULL,modifications varchar(500) NOT NULL,seq_ratio float NOT NULL,cluster_fk varchar(100) NOT NULL,PRIMARY KEY (id),CONSTRAINT FK_"+prj_id+" FOREIGN KEY (cluster_fk) REFERENCES "+cluster_table_name+"(cluster_id))ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
        db_excute(create_spec_t)

def Create_Insert_SpeciesExcelInfo():
    db = pymysql.connect(host="localhost", user='root', password='123456', port=3306, db='pride_cluster',
                         charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()
    try:
        #create_t_sql = "create table species_info (id int(10) NOT NULL AUTO_INCREMENT,taxIds varchar(100),scientific_name varchar(500) NOT NULL,Genus_or_Species varchar(200),primary key(id))ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
        #cursor.execute(create_t_sql)
        insert_sql = "insert into species_info (taxIds, scientific_name, Genus_or_Species) values(%s,%s,%s)"
        df = pd.read_excel('./data/species_info.xlsx',keep_default_na=False)
        DataSet = df[["taxIds", "scientific_name", "Genus_or_Species"]]
        tups = [tuple(x) for x in DataSet.values]
        cursor.executemany(insert_sql,tups)
        cursor.close()
        db.commit()
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

def Load_infile_cluster_and_spec(cluster_file_name):
    db = pymysql.connect(host="localhost",user='root',password='123456',port=3306,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

    cursor = db.cursor()
    try:
        load_cluster_sql = "load data infile '"+ cluster_file_name +"' replace into table cluster_infile_test fields terminated by ',' enclosed by '\"' lines terminated by '\\n'(cluster_id, cluster_ratio, n_spec, n_id, n_unid, sequences_ratios,sequences_mods,spectra_title, consensus_mz, consensus_intens,conf_sc);"
        cursor.execute(load_cluster_sql)

        prj_ids = exert_prj_id()
        for id in prj_ids:
            prj_id = "T_CLUSTER_SPEC_" + id
            load_spectrum_sql = "load data infile 'result\\" + prj_id + "' replace into table 'T_CLUSTER_SPEC" + prj_id + "'terminated by ',' enclosed by '\"' lines terminated by '\\n'(spectrum_title, charge, precursor_mz, taxids,is_spec_identified, id_sequences, modifications, seq_ratio,cluster_fk);"
            cursor.execute(load_spectrum_sql)
        db.commit()
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

def comparer(prj,list1,list2):

    file = open("./Data/TaxonInfo", "a+", encoding='utf-8')
    for i in range(len(list1)):
        s = str(prj) + "," + str(list2[i]) + "," + str(list1[i])
        file.write(s + "\n")
    file.close()

def csv_to_xlsx_pd():
    csv = pd.read_csv('./data/Taxonomy_Info_02.csv', encoding='utf-8')
    csv.to_excel('./data/Taxonomy_Info_02.xlsx', sheet_name='data')

def Analyser():
    db = pymysql.connect(host="localhost", user='root', password='123456', port=3306, db='pride_cluster',
                         charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()
    try:
        prj_list = exert_prj_id()
        for i in range(len(prj_list)):
            prj = prj_list[i]
            print(prj)
            groupBy_sql = "select taxids from t_cluster_spec_"+prj+" group by taxids;"
            cursor.execute(groupBy_sql)
            groupBy_data = cursor.fetchall()
            species_name_list = []
            if groupBy_data == ():
                continue
            else:
                for tax_ids in groupBy_data:
                    taxid = tax_ids.get("taxids")
                    if taxid == ''or taxid == 'unknown'or taxid == "null":
                        continue
                    else:
                        ids = str(taxid).split(",")
                        for id in ids:
                            if id == "NCBITaxon:630":
                                id = '630'
                            search_species_name_sql = "select scientific_name from species_info where taxids = " + id + ";"
                            cursor.execute(search_species_name_sql)
                            species_name_data = cursor.fetchall()
                            dict = {}
                            if len(species_name_data) != 0:
                                species_name_data[0].get("scientific_name")
                                dict.setdefault(taxid, species_name_data[0].get("scientific_name"))
                                species_name_list.append(dict)
                            else:
                                dict.setdefault(taxid, "NULL")
                                species_name_list.append(dict)

            list = []
            select_prj_sql = "select tax_id,species_name from PRJ_TAXIDS_SPECIES_02 where prj_id =\'" + prj + "\';"
            cursor.execute(select_prj_sql)
            dataSet = cursor.fetchall()
            for data in dataSet:
                dict = {}
                tax_id = data.get("tax_id")
                if tax_id == '':
                    tax_id = "NULL"
                    species_name = "NULL"
                else:
                    tax_id = data.get("tax_id")
                    species_name = data.get("species_name")
                dict.setdefault(tax_id,species_name)
                list.append(dict)

            if len(species_name_list) > len(list):
                l = len(species_name_list) - len(list)
                for i in range(l):
                    list.append("0");
                #comparer(prj,list,species_name_list)
            elif len(species_name_list) < len(list):
                l = len(list) - len(species_name_list)
                for i in range(l):
                    species_name_list.append("0")
                #comparer(prj, list, species_name_list)
            prj_l = []
            prj_l.append(prj)
            for i in range(len(list)-1):
                prj_l.append(" ")

            dataframe = pd.DataFrame({"Projects":prj_l ,"NCBI_Taxonomy_Info":species_name_list,"EBI_Taxonomy_Info":list},columns=["Projects","NCBI_Taxonomy_Info","EBI_Taxonomy_Info"])
            #dataframe.to_csv("./data/Taxonomy.csv", index=False, mode="a+", sep=',', header=0)
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

def Select():
    db = pymysql.connect(host="localhost", user='root', password='123456', port=3306, db='pride_cluster',
                         charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()
    try:
        prj_list = exert_prj_id()
        for i in range(len(prj_list)):
            prj = prj_list[i]
            select_prj_sql = "select tax_id from PRJ_TAXIDS_SPECIES where prj_id =\'"+ prj +"\';"
            cursor.execute(select_prj_sql)
            taxid_data = cursor.fetchall()
            if len(taxid_data) > 1:
                for j in range(len(taxid_data)):
                    taxids = taxid_data[j]
                    taxid = taxids.get("tax_id")
                    id = str(taxid).strip("\"")
                    select_sql = "select taxids from T_CLUSTER_SPEC_" + prj + " where taxids != \"" + id + "\";"
                    cursor.execute(select_sql)
                    data = cursor.fetchall()
                    id_list = []
                    if len(data) != 0:
                        for id in data:
                            value = id.get("taxids")
                            val = str(value).split(",")
                            for v in val:
                                id_list.append(v)
                idSet = set(id_list)
                if len(idSet) != 0:
                        print(prj)
                        print(taxid_data)
                        print(idSet)

            elif taxid_data == ():
                continue
            else:
                taxid = taxid_data[0].get("tax_id")
                if taxid == '':
                    continue
                taxid = str(taxid).strip("\"")
                select_sql = "select taxids from T_CLUSTER_SPEC_" + prj + " where taxids !=" + taxid + ";"
                cursor.execute(select_sql)
                data = cursor.fetchall()
                id_list = []
                if len(data) == 0:
                    continue
                else:
                    for id in data:
                        value = id.get("taxids")
                        val = str(value).split(",")
                        for v in val:
                            id_list.append(v)
                idSet = set(id_list)
                if len(idSet) != 0:
                    print(prj)
                    print(taxid_data)
                    print(idSet)
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

if __name__ == '__main__':

    start_t  = time.clock()
    csv_to_xlsx_pd()
    end_t = time.clock()
    run_t = end_t - start_t
    print(run_t)
