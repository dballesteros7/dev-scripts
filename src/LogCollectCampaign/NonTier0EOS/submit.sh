#!/bin/bash

if [ "x" != "x$VO_CMS_SW_DIR" ]
then
	. $VO_CMS_SW_DIR/cmsset_default.sh
fi

if [ -e $VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh ]
then
	. $VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh 
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib
fi

rfcp vocms15.cern.ch:/data/dballest/logCollection-nonTier0-EOS/AssembleTarball.py .
rfcp vocms15.cern.ch:/data/dballest/logCollection-nonTier0-EOS/workflowCatalog-MONTH/REPLACEME .

python2.6 AssembleTarball.py WORKFLOW MONTH

rfcp REPLACEME.done vocms15.cern.ch:/data/dballest/logCollection-nonTier0-EOS/workflowCatalog-MONTH/REPLACEME.done

exit 0
