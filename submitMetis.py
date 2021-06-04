import os
import commands

from metis.Sample import DBSSample
from metis.LocalMergeTask import LocalMergeTask
from metis.CondorTask_V2 import CondorTask_V2 as CondorTask
from metis.StatsParser import StatsParser
import samples_V2 as samples
import argparse

import time
from time import sleep
import sys

# grid password
grid_password = ''
condorpath = os.path.dirname(os.path.realpath(__file__))
os.system("echo "+grid_password+" | voms-proxy-init -voms cms -valid 192:00;cp /tmp/x509up_u123238 "+condorpath) 

# Avoid spamming too many short jobs to condor
# Less dileptn pairs = faster = more input files per job
def split_func(dsname):
    if "Run201" in dsname:
        return 7
    else:
        return 1
    # TODO: To be implemented later
    # if any(x in dsname for x in [
    #     "/W","/Z","/TTJets","/DY","/ST",
    #     ]):
    #     return 5
    # elif "Run201" in dsname:
    #     return 7
    # else:
    #     return 2


 
def Create_Submit_Scripts():
    path = condorpath+"/tasks/"
    files=os.listdir(path)
    outputfiles = "submit_"+time.strftime("y%Y_m%m_d%d_H%H_M%M_S%S", time.localtime())+".sh"
    output = ""
    for i in files:
        i = i.replace(" ","").replace("\n","")
        if "Condor" in i:
            submit_str = "condor_submit tasks/"+i+"/submit.cmd"
            output += submit_str+"\n"
        
    with open(outputfiles,"w") as f:
        f.write(output)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Submit jobs for VVV analysis")
    parser.add_argument('-m' , '--mode'        , dest='mode'      , help='tag of the job'            , type=int, required=True                                     )
    parser.add_argument('-a' , '--addflags'    , dest='addflags'  , help='adding flags to metis'     , type=str,                default=""                         )
    parser.add_argument('-d' , '--datamc'      , dest='datamc'    , help='data or mc'                ,                          default=False , action='store_true')
    parser.add_argument('-y' , '--year'        , dest='year'      , help='data year'                 , type=int, required=True                                     )
    parser.add_argument('-t' , '--thetag'      , dest='thetag'    , help='tag'                       , type=str,                default="test"                     )
    parser.add_argument('-s' , '--mysample'    , dest='mysample'  , help='don\'t do autmoated sample',                          default=False , action='store_true')
    parser.add_argument('-dr' , '--dryrun'    , dest='dryrun'  , help='only generate submit file',                          default=True , action='store_true')
    parser.add_argument('-du' , '--test'    , dest='test'  , help='only generate submit file',                          default=False , action='store_true')
    # Argument parser
    args = parser.parse_args()
    args.mode
    
    # Specify a dataset name and a short name for the output root file on nfs
    sample_map = samples.mc_2018 # See condor/samples.py
    if args.year==2017:
        sample_map = samples.mc_2017 # See condor/samples.py
    elif args.year==2016:
        sample_map = samples.mc_2016 # See condor/samples.py
    if args.datamc:
        sample_map = samples.data_2018 # See condor/samples.py
        if args.year==2017:
            sample_map = samples.data_2017 # See condor/samples.py
        elif args.year==2016:
            sample_map = samples.data_2016 # See condor/samples.py
    if args.test:
        sample_map = samples.test # See condor/samples.py
        

    # submission tag
    tag = args.thetag 

    # Where the merged output will go
    # merged_dir = "/nfs-7/userdata/{}/tupler_babies/merged/VVV/{}/output/{}".format(os.getenv("USER"),tag,args.year)
    merged_dir = "/eos/user/q/qiguo/"

    # Task summary for printing out msummary
    task_summary = {}

    if args.dryrun :
        for ds,shortname in sample_map.items():
            # skip_tail = True
            skip_tail = False
            print(shortname)
            task = CondorTask(
                    sample = ds,
                    files_per_output = split_func(ds.get_datasetname()),
                    output_name = "output.root",
                    tag = tag,
                    output_dir = "/eos/user/q/qiguo/PKUVVV/NanoAOD/Lepton1/Test/",
                    condor_submit_params = {
                        "sites": "T2_US_UCSD,UAF",
                        "use_xrootd":True,
                        "requireHAS_SINGULARITY" : False,
                        "classads": [
                            # need to add quota here \"
                            # ["metis_extraargs", "\"--mode {} {}\"".format(args.mode,args.addflags)]
                            # for MaxRuntime, this is required to be integer
                            ["MaxRuntime", "86400"]
                            ]
                        },
                    cmssw_version = "CMSSW_10_0_0",
                    scram_arch = "slc7_amd64_gcc700",
                    input_executable = "{}/condor_executable_metis.sh".format(condorpath), # your condor executable here
                    tarfile = "{}/package.tar.gz".format(condorpath), # your tarfile with assorted goodies here
                    special_dir = "VVVAnalysis/{}/{}".format(tag,args.year), # output files into /hadoop/cms/store/<user>/<special_dir>
                    min_completion_fraction = 0.50 if skip_tail else 1.0,
                    # additional arguments are passed in the arguments
                    # how the white space is treated in the condor:
                    # https://research.cs.wisc.edu/htcondor/manual/v7.7/condor_submit.html
                    arguments = "'--mode {} -w write'".format(args.mode),
                    # max_jobs = 10,
                    # add additional_input_files
                    # proxypath should be the full path of the file that you want Condor to ship with the job, its value has to be the full resolved AFS path, i.e. it cannot be $HOME/private/x509up, nor ~/private/x509, nor a path on /eos/.
                    # https://batchdocs.web.cern.ch/tutorial/exercise2e_proxy.html
                    additional_input_files = ['{}/{}'.format( condorpath , "x509up_u{0}".format(os.getuid()) )],
            )
            task.process()
            
    Create_Submit_Scripts()
    sys.exit(0)

    # Infinite loop until all tasks are complete
    # It will sleep every 10 minutes (600 seconds) and re-check automatically
    # while True:

    #     # Boolean to aggregate whether all tasks are complete
    #     all_tasks_complete = True

    #     # Loop over the dataset provided by the user few lines above, and do the Metis magic
    #     for ds,shortname in sample_map.items():
    #         # skip_tail = True
    #         skip_tail = False
    #         task = CondorTask(
    #                 sample = ds,
    #                 files_per_output = split_func(ds.get_datasetname()),
    #                 output_name = "output.root",
    #                 tag = tag,
    #                 condor_submit_params = {
    #                     "sites": "T2_US_UCSD,UAF",
    #                     "use_xrootd":True,
    #                     "classads": [
    #                         ["metis_extraargs", "--mode {} {}".format(args.mode,args.addflags)]
    #                         ]
    #                     },
    #                 cmssw_version = "CMSSW_10_0_0",
    #                 scram_arch = "slc7_amd64_gcc700",
    #                 input_executable = "{}/condor_executable_metis.sh".format(condorpath), # your condor executable here
    #                 tarfile = "{}/package.tar.xz".format(condorpath), # your tarfile with assorted goodies here
    #                 special_dir = "VVVAnalysis/{}/{}".format(tag,args.year), # output files into /hadoop/cms/store/<user>/<special_dir>
    #                 min_completion_fraction = 0.50 if skip_tail else 1.0,
    #                 # max_jobs = 10,
    #         )
    #         # When babymaking task finishes, fire off a task that takes outputs and merges them locally (hadd)
    #         # into a file that ends up on nfs (specified by `merged_dir` above)
    #         merge_task = LocalMergeTask(
    #                 input_filenames=task.get_outputs(),
    #                 output_filename="{}/{}.root".format(merged_dir,shortname),
    #                 ignore_bad = skip_tail,
    #                 )
    #         # Straightforward logic
    #         if not task.complete():
    #             task.process()
    #         else:
    #             if not merge_task.complete():
    #                 merge_task.process()

    #         # Aggregate whether all tasks are complete
    #         all_tasks_complete = all_tasks_complete and task.complete()

    #         # Set task summary
    #         task_summary[task.get_sample().get_datasetname()] = task.get_task_summary()


    #     # Parse the summary and make a summary.txt that will be used to pretty status of the jobs
    #     os.system("rm web_summary.json")
    #     webdir="~/public_html/VVVNanoLooperDashboard{}".format(args.year)
    #     StatsParser(data=task_summary, webdir=webdir).do()
    #     os.system("chmod -R 755 {}".format(webdir))
    #     os.system("msummary -r -i {}/web_summary.json".format(webdir))

    #     # If all done exit the loop
    #     if all_tasks_complete:
    #         print("")
    #         print("All job finished")
    #         print("")
    #         break

    #     # Neat trick to not exit the script for force updating
    #     print('Press Ctrl-C to force update, otherwise will sleep for 600 seconds')
    #     try:
    #         for i in reversed(range(0, 600)):
    #             sleep(1) # could use a backward counter to be preeety :)
    #             sys.stdout.write("\r{} mins {} seconds till updating ...".format(i/60, i%60))
    #             sys.stdout.flush()
    #     except KeyboardInterrupt:
    #         raw_input("Press Enter to force update, or Ctrl-C to quit.")
    #         print("Force updating...")

