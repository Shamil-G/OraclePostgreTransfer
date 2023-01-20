from db.ps_conneсt import select, set_delivered
from db.ora_connect import get_connection, plsql_execute, plsql_proc, debug_level
from util.logger import log


stmt_incoming = """
select  gfss_in_nom, to_char(gfss_in_date,'dd.mm.yyyy hh24:MI:SS') gfss_in_date, 
		gcvp_out_nom, to_char(gcvp_out_date,'dd.mm.yyyy hh24:MI:SS') gcvp_out_date,
		gfss_out_nom, to_char(gfss_out_date,'dd.mm.yyyy hh24:MI:SS') gfss_out_date,
		to_char(dat_reg,'dd.mm.yyyy hh24:MI:SS') dat_reg, 
		gcvp_order_nom, to_char(gcvp_order_date,'dd.mm.yyyy hh24:MI:SS') gcvp_order_date,
		id_doc
from oracle.gfss_journal
where delivery=false
"""

def stmt_set_delivery(GFSS_IN_NOM):
    return f"""
        update oracle.gfss_journal set delivery=true,
	    date_delivery=current_timestamp 
        where gfss_in_nom='{GFSS_IN_NOM}'
    """


def stmt_insert(iGFSS_IN_NOM, iGFSS_IN_DATE,
    iGCVP_OUT_NOM, iGCVP_OUT_DATE,
    iGFSS_OUT_NOM, iGFSS_OUT_DATE,
    iDAT_REG,
    iGCVP_ORDER_NOM, iGCVP_ORDER_DATE,
    iid_doc):
   return f"""
INSERT INTO gfss_journal
    (ID,
    GFSS_IN_NOM, GFSS_IN_DATE,
    GCVP_OUT_NOM, GCVP_OUT_DATE,
    GFSS_OUT_NOM, GFSS_OUT_DATE,
    DAT_REG,
    GCVP_ORDER_NOM, GCVP_ORDER_DATE,
    id_doc, rec_state
    )
VALUES
    (SEQ_JOURNAL.NEXTVAL,
    {iGFSS_IN_NOM}, to_date('{iGFSS_IN_DATE}','dd.mm.yyyy hh24:MI:SS'),
    {iGCVP_OUT_NOM}, to_date('{iGCVP_OUT_DATE}','dd.mm.yyyy hh24:MI:SS'),
    {iGFSS_OUT_NOM}, to_date('{iGFSS_OUT_DATE}','dd.mm.yyyy hh24:MI:SS'),
    to_date('{iDAT_REG}','dd.mm.yyyy hh24:MI:SS'),
    {iGCVP_ORDER_NOM}, to_date('{iGCVP_ORDER_DATE}','dd.mm.yyyy hh24:MI:SS'),
    {iid_doc}, 0
    )
"""



def get_and_put():
    mistake, records, err_mess = select(stmt_incoming)
    cnt = 0
    if mistake == 0:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for rec in records:
                    gfss_in_nom = rec[0]
                    gfss_in_date = rec[1]
                    gcvp_out_nom = rec[2]
                    gcvp_out_date = rec[3]
                    gfss_out_nom = rec[4]
                    gfss_out_date = rec[5]
                    dat_reg = rec[6]
                    gcvp_order_nom = rec[7]
                    gcvp_order_date = rec[8]
                    id_doc = rec[9]
                    stmt = stmt_insert(gfss_in_nom, gfss_in_date, gcvp_out_nom, gcvp_out_date, gfss_out_nom, gfss_out_date, dat_reg, gcvp_order_nom, gcvp_order_date, id_doc)
                    args = (gfss_in_nom, gfss_in_date, gcvp_out_nom, gcvp_out_date, gfss_out_nom, gfss_out_date, dat_reg, gcvp_order_nom, gcvp_order_date, id_doc)
                    if int(debug_level) > 2:
                        log.info(f'\n+++++ INSERT INTO ORACLE.\n{stmt}\n' )
                        #log.info(f'GET AND PUT.\nGFSS_IN_NOM: {gfss_in_nom}\nGFSS_IN_DATE: {gfss_in_date}\nGCVP_OUT_NOM: {gcvp_out_nom}\nGCVP_OUT_DATE: {gcvp_out_date}' 
                        #         f'\nGFSS_OUT_NOM: {gfss_out_nom}\nGFSS_OUT_DATE: {gfss_out_date}\nDAT_REG: {dat_reg}\nGCVP_ORDER_NOM: {gcvp_order_nom}'
                        #         f'\nGCVP_ORDER_DATE: {gcvp_order_date}\nID_DOC: {id_doc}')
                    #mistake = cursor.execute(stmt)
                    mistake = plsql_proc(cursor, 'GET AND PUT', 'doc_import_service', args)
                    log.info(f'+++++ GET AND PUT. MISTAKE: {mistake}' )
                    if mistake is None or mistake == 0:
                        conn.commit()
                        conn.commit()
                        stmt = stmt_set_delivery(gfss_in_nom)
                        mistake = set_delivered(stmt)
                        cnt = cnt + 1
                    else:
                        break
    log.info(f'Загружено {cnt} записей')
    log.info(f'-----------------------')


def send_all():
    get_and_put()


if __name__ == "__main__":
    send_all()
