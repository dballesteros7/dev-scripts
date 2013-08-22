'''
Created on Jan 19, 2013

@author: dballest
'''
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter
import time
import logging
import os
import sys
import shutil

mergedRAWforRun = """SELECT merged_files.lfn, merged_files.events
                     FROM run
                     INNER JOIN run_stream_fileset_assoc rsfa ON
                         rsfa.run_id = run.run_id
                     INNER JOIN stream ON
                         rsfa.stream_id = stream.id
                     INNER JOIN wmbs_subscription ON
                         wmbs_subscription.fileset = rsfa.fileset
                     INNER JOIN wmbs_workflow ON
                         wmbs_subscription.workflow = wmbs_workflow.id
                     INNER JOIN wmbs_workflow_output ON
                         wmbs_workflow_output.workflow_id = wmbs_workflow.id
                     INNER JOIN wmbs_fileset merged_out ON
                         merged_out.id = wmbs_workflow_output.merged_output_fileset
                     INNER JOIN wmbs_fileset_files merged_files_assoc ON
                         merged_files_assoc.fileset = merged_out.id
                     INNER JOIN wmbs_file_details merged_files ON
                         merged_files.id = merged_files_assoc.fileid
                     WHERE run.run_id = :run AND
                           stream.name = 'A'
                 """
unmergedRAWforRun = """SELECT unmerged_files.lfn, unmerged_files.events
                     FROM run
                     INNER JOIN run_stream_fileset_assoc rsfa ON
                         rsfa.run_id = run.run_id
                     INNER JOIN stream ON
                         rsfa.stream_id = stream.id
                     INNER JOIN wmbs_subscription ON
                         wmbs_subscription.fileset = rsfa.fileset
                     INNER JOIN wmbs_workflow ON
                         wmbs_subscription.workflow = wmbs_workflow.id
                     INNER JOIN wmbs_workflow_output ON
                         wmbs_workflow_output.workflow_id = wmbs_workflow.id
                     INNER JOIN wmbs_fileset unmerged_out ON
                         unmerged_out.id = wmbs_workflow_output.output_fileset
                     INNER JOIN wmbs_fileset_files unmerged_files_assoc ON
                         unmerged_files_assoc.fileset = unmerged_out.id
                     INNER JOIN wmbs_file_details unmerged_files ON
                         unmerged_files.id = unmerged_files_assoc.fileid
                     LEFT OUTER JOIN wmbs_sub_files_complete ON
                         wmbs_sub_files_complete.fileid = unmerged_files.id
                     WHERE run.run_id = :run AND
                           stream.name = 'A' AND
                           wmbs_sub_files_complete.fileid is NULL
                             """
                             
runInfo = """SELECT run.run_id, run.lumicount
             FROM run
             WHERE run.run_id = :run
          """


class TheDAO(DBFormatter):
    
    def execute(self, run, lumiCountOffset = 0, conn = None, transaction = False):
        binds = {'run' : run}
        merged_results = self.dbi.processData(mergedRAWforRun, binds, conn = conn, transaction = transaction)
        unmerged_results = self.dbi.processData(unmergedRAWforRun, binds, conn = conn, transaction = transaction)
        runInfo_results = self.dbi.processData(runInfo, binds, conn = conn, transaction = transaction)
        
        merged_results_formatted = self.formatDict(merged_results)
        unmerged_results_formatted = self.formatDict(unmerged_results)
        runInfo_formatted = self.formatDict(runInfo_results)
        
        pdInfo = {}
        
        for entry in merged_results_formatted:
            lfn = entry['lfn']
            if 'logArchive' in lfn:
                continue
            pd = lfn.split('/')[4]
            if pd not in pdInfo:
                pdInfo[pd] = 0
            pdInfo[pd] += entry['events']
            
        for entry in unmerged_results_formatted:
            lfn = entry['lfn']
            if 'logArchive' in lfn:
                continue
            pd = lfn.split('/')[5]
            if pd not in pdInfo:
                pdInfo[pd] = 0
            pdInfo[pd] += entry['events']
            
        if not int(runInfo_formatted[0]['lumicount']):
            return {}
            
        rateInfo = {}
        for pd in pdInfo:
            if 'PA' in pd:
                rateInfo[pd] = float(pdInfo[pd])/(23.31*(int(runInfo_formatted[0]['lumicount']) - lumiCountOffset))
            else:
                rateInfo[pd] = float(pdInfo[pd])/(23.31*(int(runInfo_formatted[0]['lumicount'])))
            
        return rateInfo
    
class aSecondDAO(DBFormatter):
    sql = """SELECT run_id 
              FROM (SELECT run.run_id, Row_Number() OVER (ORDER BY run_id DESC) MyRow 
                    FROM run
                   )
              WHERE MyRow <= 20
              ORDER BY run_id
           """
    
def main():
    
    dbFactoryT0AST = DBFactory(logging, dburl = 'oracle://FOO:BAR@GOOGLE', options = {})
    dbInterfaceT0AST = dbFactoryT0AST.connect()
    dao = TheDAO(logging, dbInterfaceT0AST)
    runsDao = aSecondDAO(logging, dbInterfaceT0AST)
    runs = runsDao.execute()
    
    lumiFileHandle = open('/afs/cern.ch/user/d/dballest/public/t0/HILumis.txt', 'r')
    lumiOffsets = {}
    for line in lumiFileHandle:
        tokens = line.split()
        run = tokens[0]
        lumioff = tokens[1]
        lumiOffsets[run] = lumioff
    lumiFileHandle.close()
    
    rateInfo = {}
    for run in runs:
        results = dao.execute(run = run[0], lumiCountOffset = int(lumiOffsets.get(str(run[0]), 0)))
        if results:
            rateInfo[str(run[0])] = results
        
    theFile = '/afs/cern.ch/user/c/cmsprod/www/pdRates.txt'
    tmpFileHandle = open('/tmp/pdRates.tmp', 'w')
    tmpFileHandle.write('Rate information\n')
    currentTime = time.strftime('%d-%m-%y %H:%M %Z')
    tmpFileHandle.write('Updated on %s\n' % currentTime)
    for run in sorted(rateInfo.keys(), reverse = True):
        tmpFileHandle.write('Run: %s\n' % run)
        for PD in rateInfo[run]:
            tmpFileHandle.write('PD: %15s Event Rate: %4.2f Hz\n' % (PD, rateInfo[run][PD]))
        tmpFileHandle.write('=====================================================\n')
        tmpFileHandle.write('=====================================================\n')
    
    tmpFileHandle.close()
    shutil.move('/tmp/pdRates.tmp', theFile)
    
    return 0

if __name__ == '__main__':
    sys.exit(main())