package com.hmdu.bigdata.hbaseobserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:42 2018/7/13
 * @
 */
public class ObserverLog extends BaseRegionObserver {
    private static Logger logger = Logger.getLogger(ObserverLog.class);

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String rowkey = Bytes.toString(put.getRow());

        CellScanner cellScanner = put.cellScanner();

        StringBuffer sb = new StringBuffer();
        sb.append(rowkey);

        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();

            sb.append("\t" + new String(CellUtil.cloneFamily(cell), "utf-8")
                    + ":" + new String(CellUtil.cloneQualifier(cell), "utf-8")
                    + " " + new String(CellUtil.cloneValue(cell), "utf-8"));
        }

        logger.info(sb.toString());

        super.prePut(e, put, edit, durability);
    }
}
