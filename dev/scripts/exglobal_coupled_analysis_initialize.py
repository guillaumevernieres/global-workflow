#!/usr/bin/env python3
# exglobal_coupled_analysis_initialize.py
# This script initializes coupled atmosphere-marine JEDI data assimilation
# by calling both AtmAnalysis and MarineAnalysis initialize methods
# which creates and stages the runtime directories
# and creates the YAML configurations for coupled DA
import os

from wxflow import Logger, cast_strdict_as_dtypedict
from pygfs.task.atm_analysis import AtmAnalysis
from pygfs.task.marine_analysis import MarineAnalysis

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config = cast_strdict_as_dtypedict(os.environ)

    # Instantiate the atmosphere analysis task
    logger.info("Initializing coupled DA: atmosphere component")
    AtmAnl = AtmAnalysis(config)
    AtmAnl.initialize()

    # Instantiate the marine analysis task
    logger.info("Initializing coupled DA: marine component")
    MarineAnl = MarineAnalysis(config)
    MarineAnl.initialize()

    logger.info("Coupled DA initialization complete")
