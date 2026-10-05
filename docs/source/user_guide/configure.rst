=============
Configure Run
=============

The GW configs contain switches that change how the system runs. Many defaults are set initially. Users wishing to run with different settings should adjust their $EXPDIR configs and then rerun the ``setup_workflow.py`` script since some configuration settings/switches change the workflow/xml ("Adjusts XML" column value is "YES").

+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| Switch           | What                             | Default       | Adjusts XML | More Details                                      |
+==================+==================================+===============+=============+===================================================+
| APP              | Model application                | ATM           | YES         | See case block in config.base for options         |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DEBUG_POSTSCRIPT | Debug option for PBS scheduler   | NO            | YES         | Sets debug=true for additional logging            |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DOIAU            | Enable 4DIAU for control         | YES           | NO          | Turned off for cold-start first half cycle        |
|                  | with 3 increments                |               |             |                                                   |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DOHYBVAR         | Run EnKF                         | YES           | YES         | Don't recommend turning off                       |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DONST            | Run NSST                         | YES           | NO          | If YES, turns on NSST in anal/fcst steps, and     |
|                  |                                  |               |             | turn off rtgsst                                   |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_AWIPS         | Run jobs to produce AWIPS        | NO            | YES         | downstream processing, ops only                   |
|                  | products                         |               |             |                                                   |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_BUFRSND       | Run job to produce BUFR          | NO            | YES         | downstream processing                             |
|                  | sounding products                |               |             |                                                   |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_GEMPAK        | Run job to produce GEMPAK        | NO            | YES         | downstream processing, ops only                   |
|                  | products                         |               |             |                                                   |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_FIT2OBS       | Run FIT2OBS job                  | YES           | YES         | Whether to run the FIT2OBS job                    |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_TRACKER       | Run tracker job                  | YES           | YES         | Whether to run the tracker job                    |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_GENESIS       | Run genesis job                  | YES           | YES         | Whether to run the genesis job                    |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_GENESIS_FSU   | Run FSU genesis job              | YES           | YES         | Whether to run the FSU genesis job                |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_VERFOZN       | Run GSI monitor ozone job        | YES           | YES         | Whether to run the GSI monitor ozone job          |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_VERFRAD       | Run GSI monitor radiance job     | YES           | YES         | Whether to run the GSI monitor radiance job       |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_VMINMON       | Run GSI monitor minimization job | YES           | YES         | Whether to run the GSI monitor minimization job   |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_ANLSTAT       | Run analysis statistics job      | NO            | YES         | Whether to run the analysis statistics job.       |
|                  |                                  |               |             | Automatically set to YES for JEDI-based           |
|                  |                                  |               |             | experiments (DO_JEDIATMVAR, DO_AERO,              |
|                  |                                  |               |             | DO_JEDIOCNVAR, or DO_JEDISNOWDA).                 |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_GSI_ANLSTAT   | Run GSI analysis statistics job  | NO            | NO          | Whether to include GSI-based atmospheric analysis |
|                  |                                  |               |             | statistics when running the anlstat job. Only     |
|                  |                                  |               |             | relevant when DO_ANLSTAT=YES and using GSI (not   |
|                  |                                  |               |             | JEDI) for atmospheric data assimilation.          |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_METP          | Run METplus jobs                 | YES           | YES         | One cycle spinup                                  |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| EXP_WARM_START   | Is experiment starting warm      | .false.       | NO          | Impacts IAU settings for initial cycle. Can also  |
|                  | (.true.) or cold (.false)?       |               |             | be set when running ``setup_expt.py`` script with |
|                  |                                  |               |             | the ``--start`` flag (e.g. ``--start warm``)      |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| DO_ARCHCOM       | Archive COM                      | YES/NO        | YES         | Whether to archive the COM structure.  Defaults   |
|                  |                                  |               |             | are machine-specific.                             |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| ARCHCOM_TO       | Where to archive COM             | hpss, local,  | YES         | If DO_ARCHCOM is YES, then this variable indicates|
|                  |                                  | or globus_hpss|             | where the COM structure tarballs should be saved. |
|                  |                                  |               |             | Choices are 'hpss', 'local', or 'globus_hpss'.    |
|                  |                                  |               |             | HPSS archiving requires a direct connection.      |
|                  |                                  |               |             | Globus-HPSS archiving uses Mercury as a server to |
|                  |                                  |               |             | archiving to HPSS.  This is currently only        |
|                  |                                  |               |             | supported on Hercules.  Defaults are machine      |
|                  |                                  |               |             | specific.                                         |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| ARCH_EXPDIR      | Archive the EXPDIR               | NO            | NO          | Whether to create a tarball of the EXPDIR.        |
|                  |                                  |               |             | ARCH_HASHES and ARCH_DIFFS generate text files    |
|                  |                                  |               |             | of git output that are archived with the EXPDIR.  |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| QUILTING         | Use I/O quilting                 | .true.        | NO          | If .true. choose OUTPUT_GRID as cubed_sphere_grid |
|                  |                                  |               |             | in netcdf or gaussian_grid                        |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| CAT_MPMD_LOGS    | Write MPMD logs back to the      | YES           | NO          | If YES, the contents of the MPMD logs will be     |
|                  | parent log.                      |               |             | written to the parent log file.                   |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| WRITE_DOPOST     | Run inline post                  | .true.        | NO          | If .true. produces master post output in forecast |
|                  |                                  |               |             | job                                               |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+
| USE_BUILD_GSINFO | Build the GSI info files         | YES           | NO          | If YES, the GSI analysis jobs will build the      |
|                  |                                  |               |             | satinfo, cnvinfo, and ozinfo files dynamically.   |
|                  |                                  |               |             | If NO, static versions located in the GSI FIX     |
|                  |                                  |               |             | directory will be used.                           |
+------------------+----------------------------------+---------------+-------------+---------------------------------------------------+

JEDI marine model interface selection
-------------------------------------

When running the JEDI-based marine variational analysis, the model interface used by the
variational solver can be selected (the default reproduces the current system):

* ``JEDI_MARINE_INTERFACE`` (``base`` section): ``soca`` (default) or ``ijedi``. With
  ``ijedi``, the marine variational step runs ``gdas_ijedi.x var`` on the i-jedi MOM6 geometry.
  The analysis is ocean only (no sea-ice increment) and is supported only at 5 degree ocean
  resolution (``OCNRES=500``). The MPI layout of the i-jedi MOM6 geometry is derived from the
  ``marineanlvar`` task count in ``config.resources``. The background error is identity until
  the B-matrix jobs can calibrate i-jedi diffusion parameters (soca's cannot be read by i-jedi);
  the B-matrix and increment post-processing steps remain soca-based.

This switch lives in the experiment configs (``config.base``), so changing it requires
re-running ``setup_expt.py``; it does not adjust the XML.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Custom MOM6 and CICE6 input template paths
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

During workflow installation ``link_workflow.sh`` stages the MOM6 and CICE6
input templates from ``sorc/ufs_model.fd/tests/parm`` into ``${PARMglobal}/ufs``.
The variables described in this section let an experiment take those templates from
a different directory as an optional override. Note that using these variables
leaves the corresponding templates staged in ``${PARMglobal}/ufs`` intact, but
unused.

Their defaults live in the ``ocn`` and ``ice`` sections of each net's
``yaml/defaults.yaml``, which is also where you can see the stock paths.

``MOM6_INPUT_TEMPLATE`` (``config.ocn``)
  Full path to the ``MOM_input`` template. Note that the default follows the
  ``MOM_input_<OCNRES>.IN`` pattern, e.g. ``MOM_input_025.IN``, but the override 
  file may have any name.

``MOM6_DATA_TABLE_TEMPLATE`` (``config.ocn``)
  Full path to the ``data_table`` template.

``CICE_TEMPLATE`` (``config.ice``)
  Full path to the ``ice_in`` template.

Set them in the YAML passed to ``setup_expt.py --yaml``, under the section named
for the config that owns each one (see below example). That YAML must include the
defaults with ``!INC``; keys beside ``defaults`` are merged over them, so a file
without the include would start from nothing and drop every other setting::

  defaults:
    !INC {{ HOMEglobal }}/dev/parm/config/gfs/yaml/defaults.yaml
  ocn:
    MOM6_INPUT_TEMPLATE: /path/to/my_experiment/configs/MOM_input_008.IN
  ice:
    CICE_TEMPLATE: /path/to/my_experiment/configs/ice_in.IN

See ``dev/ci/cases/yamls/gfs_defaults_ci.yaml`` for a working example of that
layout.

Because the paths live in the experiment YAML rather than in an edited ``$EXPDIR``
config, they survive re-running ``setup_expt.py`` and can be version controlled
alongside the templates they point at.

A note on atparse tokens
""""""""""""""""""""""""

These files are templates, not finished input files. They are rendered with
``atparse``, which substitutes every ``@[VARIABLE]`` token from the shell
environment.

We suggest model developers start from a copy of the base template in ``${PARMglobal}/ufs``
and keep its tokens. They are how the workflow injects per-cycle and per-job settings,
and a token deleted from a custom template fails silently: the model simply falls
back to its own compiled default. A particularly consequential case is the tokens whose
values differ by ``RUN``. As an example (though not the only example), ``@[CICE_HIST_AVG]``
sets ``hist_avg`` in CICE's ``&setup_nml``, which decides whether each history stream
is averaged over ``histfreq_n`` or written as an instantaneous snapshot.
``parsing_namelists_cice.sh`` sets it to ``.false.`` for ``gdas`` because data
assimilation uses an instantaneous history, and to ``.true.`` for the long ``gfs``
forecast.
Dropping that token gives a DA cycle time-averaged sea ice history with no error message.

A token whose variable is *undefined* behaves differently: the forecast job runs
under ``set -u``, so ``atparse`` aborts. Misspelling a token name is an error;
replacing a token with a hard-coded value is not, even when it should be.

Additional MOM6 and CICE6 input overrides
"""""""""""""""""""""""""""""""""""""""""

``input.nml`` and ``diag_table`` are rendered into the run directory from
``${PARMglobal}/ufs/global_control.nml.IN`` and ``${PARMglobal}/ufs/fv3/diag_table``
respectively, and each hold both FV3 and MOM6 content, because FMS opens only one
of each. Therefore, no MOM6-specific ``MOM6/input.nml`` exists. MOM6's share of
``input.nml`` is the ``&MOM_input_nml`` group, and its share of ``diag_table`` is
the ``"ocean_model"`` and ``"ocean_model_z"`` lines.

``MOM_layout``, ``MOM_override`` and ``MOM_channels`` are staged from
``${FIXglobal}/mom6/${OCNRES}``. Overriding this default location is possible via the fix file overrides, not by
the variables above. Also note that, by default ``MOM_layout`` is not read by MOM6:
it opens only the files listed in ``parameter_filename`` in ``&MOM_input_nml``, which includes only
``MOM_input`` and ``MOM_override`` by default.
