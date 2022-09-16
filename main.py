import db_function as db
import repair


lastIdDM = db.searchLastId_DM()
df = db.catch_lastRecordStatisticsByCients(lastIdDM)

if df.empty == False:
    df = repair.repair_columnTags(df)
    df = repair.repair_columnQuantityTags(df)
    df = repair.repair_clientName(df)
    df = repair.repair_data(df)
    df = df.reset_index(drop=True, inplace=False)
    db.insert_DMcients(df)
else:
    print('sem atualizações')
