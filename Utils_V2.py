import os
import time

from metis.Constants import Constants
from metis.Task import Task
from metis.File import EventsFile
import metis.Utils as Utils
import metis.Utils_V2 as Utils_V2
from metis.CondorTask import CondorTask


class CondorTask_V2(CondorTask):

    def __init__(self, **kwargs):
        
        self.dryrun = kwargs.get("dryrun", True)
        
        super(CondorTask_V2, self).__init__(**kwargs)

        # set the output dir
        User = os.environ.get("USER")
        default_output_dir = "/eos/user/{0}/{1}/PKUVVV/NanoAOD/{2}/".format(User[0],User,time.strftime("y%Y_m%m_d%d_H%H_M%M_S%S", time.localtime()))
        self.output_dir = kwargs.get("output_dir",default_output_dir)
        os.system("mkdir -p "+self.output_dir)



    def prepare_inputs(self):

        # need to take care of executable, tarfile
        self.executable_path = "{0}/executable.sh".format(self.get_taskdir())
        

        # take care of executable. easy.
        Utils.do_cmd("cp {0} {1}".format(self.input_executable, self.executable_path))

        # take care of package tar file if we were told to. easy.
        # copy the package tar file will waste space 
        self.package_path = self.tarfile
        # if self.tarfile:
        #     Utils.do_cmd("cp {0} {1}".format(self.tarfile, self.package_path))

        self.prepared_inputs = True

    def process(self, fake=False, optimizer=None, **kwargs):
        """
        Prepare inputs
        Execute main logic
        Backup
        """

        print(self.sample)

        dryrun = kwargs.get("dryrun", self.dryrun)

        self.logger.info("Began processing {0} ({1})".format(self.sample.get_datasetname(),self.tag))
        # set up condor input if it's the first time submitting
        if (not self.prepared_inputs) or self.recopy_inputs:
            self.prepare_inputs()


        self.run(fake=fake, optimizer=optimizer)

        if not dryrun:
            self.try_to_complete()
            if self.complete():
                self.finalize()

            self.backup()

            self.logger.info("Ended processing {0} ({1})".format(self.sample.get_datasetname(),self.tag))

    def Rename_OutFile(self, inputfile):
        return inputfile.replace("/","_")
    
    def submit_multiple_condor_jobs(self, v_ins, v_out, fake=False, optimizer=None):

        outdir = self.output_dir
        outname_noext = self.output_name.rsplit(".", 1)[0]
        v_inputs_commasep = [",".join(map(lambda x: x.get_name(), ins)) for ins in v_ins] # v_ins contains all the input dataset files
        v_index = [out.get_index() for out in v_out]
        cmssw_ver = self.cmssw_version
        scramarch = self.scram_arch
        executable = self.executable_path
        index_ins = zip(v_index,v_inputs_commasep)
        v_arguments = [[outdir, outname_noext, inputs_commasep,
                     index, cmssw_ver, scramarch, self.arguments]
                     for (index,inputs_commasep) in index_ins]
        if optimizer: 
            v_sites = optimizer.get_sites(self, v_ins, v_out)
            v_selection_pairs = [
                    [
                        ["taskname", self.unique_name],
                        ["jobnum", index],
                        ["tag", self.tag],
                        ["metis_retries", len(self.job_submission_history.get(index,[]))],
                        ["DESIRED_Sites", sites],
                        ] 
                    for index,sites in zip(v_index,v_sites)
                    ]
        else:
            # each element is for one dataset file
            v_selection_pairs = [
                    [
                        ["+taskname", self.unique_name],
                        ["+jobnum", index],
                        ["+tag", self.tag],
                        ["+metis_retries", len(self.job_submission_history.get(index,[]))],
                        ["transfer_output_remaps", "".join( ( self.output_name, ' = ' ,self.get_outputdir(), self.Rename_OutFile(dict(index_ins)[index]) ) )], # use the rename function to rename the ouputfile, dict(index_ins)[index] return the inputfiles with comma
                        ] 
                    for index in v_index
                    ]
        logdir_full = os.path.abspath("{0}/logs/".format(self.get_taskdir()))
        package_full = os.path.abspath(self.package_path)
        input_files = [package_full] if self.tarfile else []
        input_files += self.additional_input_files
        extra = self.kwargs.get("condor_submit_params", {})

        # for different service, condor submit file is different 
        condor_submit = Utils_V2.condor_submit_lxplus
        
        return condor_submit(
                    executable=executable, arguments=v_arguments,
                    inputfiles=input_files, logdir=logdir_full,
                    selection_pairs=v_selection_pairs,
                    multiple=True,
                    fake=fake, **extra
               )