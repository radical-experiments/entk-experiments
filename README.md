# EnTK experiments for IPDPS 2017

This repository contains all the EnTK scripts + input data, notebooks,
resulting plots. The raw data consisting of all the profiles are kept in 
the tarballs. All contents of this repository pertain to the work done for the
IPDPS 2017 conference.

The directory structure is as follows:

* [experiment name or number w.r.f. to the paper]
    * bin : all scripts+data required to run the experiment and generate raw 
            data (runme.py or runme.sh).     
    * notebook : scripts/methods to analyze the raw data and generate plots
    * plots : all plots generated for internal understanding + all plots for 
            paper submission
    * raw_data : all raw data generated (only in the tarball)

The notebooks and plots (pdf, png) can be opened in github to view its 
contents.


## Software stack used:

### Experiments ```5-A-2-1``` to ```5-A-2-6```:

    ```
    radical.entk         : arch/v0.6 (7a88ace6b8cf1a3e4514b5b6fd5cc7ff3a343356)
    radical.pilot        : 0.46.2
    saga                 : 0.46.1
    radical.utils        : 0.46.2
    ```

### Experiments ```5-B-1, 5-B-2```:

    ```
    radical.entk         : arch/v0.6 (7a88ace6b8cf1a3e4514b5b6fd5cc7ff3a343356)
    radical.pilot        : 0.47-v0.46.2-15-g62a193b@devel
    saga                 : 0.47-v0.46-5-g74fc381@devel
    radical.utils        : 0.47-v0.46-10-gc515db1@devel
    ```

### Experiment ```5-C-1```:

    ```
    radical.entk         : feature/gpu (3667b48a89480b92756f1e16626bbe67933bb654)
    radical.pilot        : 0.47-v0.46.2-93-g379bd68@feature-gpu  
    saga                 : 0.47-v0.46-23-g21cb4e9@feature-gpu
    radical.utils        : 0.47-v0.46-10-gc515db1@devel
    ```


## Recreate entire snapshot

The entire snapshot of the scripts, notebook, plots AND raw_data can be obtained
from the tarballs using the following instructions:

* Step 1: Download only the tarballs

    ```
    wget https://raw.githubusercontent.com/radical-experiments/entk-experiments/master/ipdps_Oct_22_2017_part1.tar.bz2
    wget https://raw.githubusercontent.com/radical-experiments/entk-experiments/master/ipdps_Oct_22_2017_part2.tar.bz2
    wget https://raw.githubusercontent.com/radical-experiments/entk-experiments/master/ipdps_Oct_22_2017_part3.tar.bz2
    ```

* Step 2: Combine the three files

    ```
    cat ipdps_Oct_22_2017_part* > ipdps_Oct_22_2017.tar.bz2
    ```

* Step 3: Untar the file (NOTE: output will be ~10GB !):

    ```
    tar xfvj ipdps_Oct_22_2017.tar.bz2
    ```


For help and questions, contact @vivek-bala (vivek.balasubramanian@rutgers.edu)
