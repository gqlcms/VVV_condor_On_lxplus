from metis.Sample import DirectorySample, DBSSample

# Master list of all samples
# Specify a dataset name and a short name for the output root file on nfs

test = {
        DBSSample(dataset="/WWW_4F_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WWW",
        DBSSample(dataset="/WZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WZZ",
}

mc_2018 = {}

mc_2016 = {
        DBSSample(dataset="/WWW_4F_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WWW",
        DBSSample(dataset="/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WJetsHT100-200",
        DBSSample(dataset="/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "WJetsHT100-200",
        DBSSample(dataset="/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext2-v1/NANOAODSIM")               : "WJetsHT100-200",
        DBSSample(dataset="/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WJetsHT200-400",
        DBSSample(dataset="/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "WJetsHT200-400",
        DBSSample(dataset="/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext2-v1/NANOAODSIM")               : "WJetsHT200-400",
        DBSSample(dataset="/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WJetsHT400-600",
        DBSSample(dataset="/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "WJetsHT400-600",
        DBSSample(dataset="/WJetsToLNu_HT-600To800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WJetsHT600-800",
        DBSSample(dataset="/WJetsToLNu_HT-600To800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "WJetsHT600-800",
        DBSSample(dataset="/WJetsToLNu_HT-800To1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WJetsHT800-1200",
        DBSSample(dataset="/WJetsToLNu_HT-800To1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "WJetsHT800-1200",
        DBSSample(dataset="/WJetsToLNu_HT-1200To2500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WJetsHT1200-2500",
        DBSSample(dataset="/WJetsToLNu_HT-1200To2500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "WJetsHT1200-2500",
        DBSSample(dataset="/WJetsToLNu_HT-2500ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WJetsHT2500-inf",
        DBSSample(dataset="/WJetsToLNu_HT-2500ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "WJetsHT2500-inf",
        DBSSample(dataset="/WWToLNuQQ_13TeV-powheg/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WW",
        DBSSample(dataset="/WWToLNuQQ_13TeV-powheg/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "WW",
        DBSSample(dataset="/WZTo1L1Nu2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WZ",
        DBSSample(dataset="/ZZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "ZZ",
        DBSSample(dataset="/TT_TuneCUETP8M2T4_13TeV-powheg-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "TT",
        DBSSample(dataset="/ST_s-channel_4f_leptonDecays_13TeV-amcatnlo-pythia8_TuneCUETP8M1/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "ST_s",
        DBSSample(dataset="/ST_t-channel_top_4f_inclusiveDecays_13TeV-powhegV2-madspin-pythia8_TuneCUETP8M1/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v2/NANOAODSIM")               : "ST_t",
        DBSSample(dataset="/ST_t-channel_antitop_4f_inclusiveDecays_13TeV-powhegV2-madspin-pythia8_TuneCUETP8M1/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "ST_t_anti",
        DBSSample(dataset="/ST_tW_antitop_5f_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "ST_tW_anti",
        DBSSample(dataset="/ST_tW_top_5f_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "ST_tW",
        DBSSample(dataset="/QCD_HT200to300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "QCD_HT200-300",
        DBSSample(dataset="/QCD_HT200to300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "QCD_HT200-300",
        DBSSample(dataset="/QCD_HT300to500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "QCD_HT300-500",
        DBSSample(dataset="/QCD_HT300to500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "QCD_HT300-500",
        DBSSample(dataset="/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "QCD_HT500-700",
        DBSSample(dataset="/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "QCD_HT500-700",
        DBSSample(dataset="/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "QCD_HT700-1000",
        DBSSample(dataset="/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "QCD_HT700-1000",
        DBSSample(dataset="/QCD_HT1000to1500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "QCD_HT1000-1500",
        DBSSample(dataset="/QCD_HT1000to1500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "QCD_HT1000-1500",
        DBSSample(dataset="/QCD_HT1500to2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "QCD_HT1500-2000",
        DBSSample(dataset="/QCD_HT1500to2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "QCD_HT1500-2000",
        DBSSample(dataset="/QCD_HT2000toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "QCD_HT2000-Inf",
        DBSSample(dataset="/QCD_HT2000toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8_ext1-v1/NANOAODSIM")               : "QCD_HT2000-Inf",
        DBSSample(dataset="/ttWJets_13TeV_madgraphMLM/RunIISummer16NanoAODv7-Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "ttW",
        DBSSample(dataset="/ttZJets_13TeV_madgraphMLM-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "ttZ",
        DBSSample(dataset="/WWZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WWZ",
        DBSSample(dataset="/WZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "WZZ",
        DBSSample(dataset="/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/NANOAODSIM")               : "ZZZ",
}


data_2016 = {
        DBSSample(dataset="/SingleMuon/Run2016B_ver2-Nano25Oct2019_ver2-v1/NANOAOD")               : "Run2016B",
        DBSSample(dataset="/SingleMuon/Run2016C-Nano25Oct2019-v1/NANOAOD")               : "Run2016C",
        DBSSample(dataset="/SingleMuon/Run2016D-Nano25Oct2019-v1/NANOAOD")               : "Run2016D",
        DBSSample(dataset="/SingleMuon/Run2016E-Nano25Oct2019-v1/NANOAOD")               : "Run2016E",
        DBSSample(dataset="/SingleMuon/Run2016F-Nano25Oct2019-v1/NANOAOD")               : "Run2016F",
        DBSSample(dataset="/SingleMuon/Run2016G-Nano25Oct2019-v1/NANOAOD")               : "Run2016G",
        DBSSample(dataset="/SingleMuon/Run2016H-Nano25Oct2019-v1/NANOAOD")               : "Run2016H",
        DBSSample(dataset="/SingleElectron/Run2016B_ver2-Nano25Oct2019_ver2-v1/NANOAOD")               : "Run2016Bele",
        DBSSample(dataset="/SingleElectron/Run2016C-Nano25Oct2019-v1/NANOAOD")               : "Run2016Cele",
        DBSSample(dataset="/SingleElectron/Run2016D-Nano25Oct2019-v1/NANOAOD")               : "Run2016Dele",
        DBSSample(dataset="/SingleElectron/Run2016E-Nano25Oct2019-v1/NANOAOD")               : "Run2016Eele",
        DBSSample(dataset="/SingleElectron/Run2016F-Nano25Oct2019-v1/NANOAOD")               : "Run2016Fele",
        DBSSample(dataset="/SingleElectron/Run2016G-Nano25Oct2019-v1/NANOAOD")               : "Run2016Gele",
        DBSSample(dataset="/SingleElectron/Run2016H-Nano25Oct2019-v1/NANOAOD")               : "Run2016Hele",
}

