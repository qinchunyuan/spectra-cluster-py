import pandas as pd
import time
import pymysql
import xlrd

def db_excute(sql):
    # local_infile = 1 执行load data infile
    db = pymysql.connect(host="localhost",user='root',password='root123123',port=3309,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

    cursor = db.cursor()
    try:
        cursor.execute(sql)
        db.commit()
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

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

def Create_Insert_SpeciesExcelInfo():
    db = pymysql.connect(host="localhost",user='root',password='root123123',port=3309,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

    cursor = db.cursor()
    try:
        create_t_sql = "create table species_info (id int(10) NOT NULL AUTO_INCREMENT,taxIds varchar(100),scientific_name varchar(500) NOT NULL,Genus_or_Species varchar(200),primary key(id))ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
        cursor.execute(create_t_sql)
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

def Create_prj_taxids_name_T():
    sql = "drop table PRJ_TAXIDS_SPECIES;"
    db_excute(sql)
    create_tabel = "create table PRJ_TAXIDS_SPECIES (id int(15) NOT NULL AUTO_INCREMENT,pr_id varchar(15) not null,tax_id varchar(10) COLLATE utf8_bin,species_name varchar(500), primary key(id))ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
    db_excute(create_tabel)

def Insert():
    db = pymysql.connect(host="localhost",user='root',password='root123123',port=3309,db='pride_cluster',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()
    try:
        df = pd.read_csv('./data/prj_taxid_name_04.csv',keep_default_na=False)
        DataSet = df[["prj_id", "tax_id", "species_name"]]
        tups = [tuple(x) for x in DataSet.values]

        insert_sql = "INSERT INTO PRJ_TAXIDS_SPECIES (prj_id,tax_id,species_name) values(%s,%s,%s);"
        cursor.executemany(insert_sql, tups)
        db.commit()

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
        for prj in prj_list:
            select_prj_sql = "select tax_id from PRJ_TAXIDS_SPECIES where project_id = "+ prj +";"
            cursor.execute(select_prj_sql)
            taxid_data = cursor.fetchall()
            if len(taxid_data) > 1:
                for taxid in taxid_data:
                    select_sql = "select taxids from T_CLUSTER_SPEC_" + prj + " where taxids != " + taxid + ";"
                    cursor.execute(select_sql)
                    data = cursor.fetchall()
                    for id in data:
                        print(id)
            db.commit()
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

def Analyser():
    db = pymysql.connect(host="localhost", user='root', password='root123123', port=3309, db='pride_cluster',
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
            select_prj_sql = "select tax_id,species_name from PRJ_TAXIDS_SPECIES where prj_id =\'" + prj + "\';"
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
            dataframe.to_csv("./data/Taxonomy_info_02.csv", index=False, mode="a+", sep=',', header=0)
    except pymysql.Error as e:
        print(e)
    finally:
        db.close()

if __name__ == '__main__':

    start_t  = time.clock()
    Analyser()
    end_t = time.clock()
    run_t = end_t - start_t
    print(run_t)
