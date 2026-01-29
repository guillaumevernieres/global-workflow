#!/bin/bash
set -eu

#####################################################################################
# Script: create-coupledda-test.sh
# Description: Manually create and launch the C48mx500_CoupledDA CI test
# Usage: ./create-coupledda-test.sh [TEST_DIR]
#
# Arguments:
#   TEST_DIR - Optional. Base directory for the test (default: /scratch3/NCEPDEV/da/${USER}/manual_coupled_da_test_${TAG})
#
# Note: If you don't have access to fv3-cpu, export HPC_ACCOUNT before running:
#   export HPC_ACCOUNT=da-cpu
#   ./create-coupledda-test.sh
#####################################################################################

# Determine HOMEgfs from script location
HOMEgfs="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse arguments
TEST_DIR_BASE="${1:-}"

# Setup environment
cd "${HOMEgfs}"
source ush/detect_machine.sh

# Load modules
if [[ -f "modulefiles/gw_setup.${MACHINE_ID}.lua" ]]; then
    module use modulefiles
    module load "gw_setup.${MACHINE_ID}"
elif [[ -f "modulefiles/gw_setup.${MACHINE_ID}" ]]; then
    module use modulefiles
    module load "gw_setup.${MACHINE_ID}"
else
    echo "WARNING: gw_setup.${MACHINE_ID} not found, continuing without loading..."
fi

source dev/ush/gw_setup.sh
source dev/ci/platforms/config.${MACHINE_ID}

# Configuration
export TAG=$(git rev-parse --short HEAD)

# Set TEST_DIR based on argument or default
if [[ -n "${TEST_DIR_BASE}" ]]; then
    export TEST_DIR="${TEST_DIR_BASE}"
else
    export TEST_DIR="/scratch3/NCEPDEV/da/${USER}/manual_coupled_da_test_${TAG}"
fi

export RUNTESTS="${TEST_DIR}/RUNTESTS"
export pslot="C48mx500_CoupledDA_${TAG}"
export ICSDIR_ROOT="/scratch3/NCEPDEV/global/role.glopara/data/ICSDIR"

echo "=========================================="
echo "Coupled DA Test Configuration"
echo "=========================================="
echo "HOMEgfs:     ${HOMEgfs}"
echo "TEST_DIR:    ${TEST_DIR}"
echo "RUNTESTS:    ${RUNTESTS}"
echo "pslot:       ${pslot}"
echo "TAG:         ${TAG}"
echo "MACHINE_ID:  ${MACHINE_ID}"
echo "=========================================="
echo ""

# Create and run
echo "Creating test directory structure..."
mkdir -p "${TEST_DIR}" "${RUNTESTS}"

echo "Creating experiment..."
python "${HOMEgfs}/dev/workflow/create_experiment.py" --overwrite --yaml "${HOMEgfs}/dev/ci/cases/pr/C48mx500_CoupledDA.yaml"

echo "Navigating to experiment directory..."
cd "${RUNTESTS}/EXPDIR/${pslot}"

echo "Launching Rocoto workflow..."
rocotorun -v 10 -w "${pslot}.xml" -d "${pslot}.db"

echo ""
echo "=========================================="
echo "Workflow launched successfully!"
echo "=========================================="
echo "Experiment directory: ${RUNTESTS}/EXPDIR/${pslot}"
echo ""
echo "To monitor progress:"
echo "  cd ${RUNTESTS}/EXPDIR/${pslot}"
echo "  rocotostat -w ${pslot}.xml -d ${pslot}.db"
echo ""
echo "To manually run rocoto:"
echo "  rocotorun -v 10 -w ${pslot}.xml -d ${pslot}.db"
echo "=========================================="