# EnTK experiments for IPDPS 2017

This repository contains all the EnTK scripts + input data, notebooks,
resulting plots. The raw data consisting of all the profiles are kept in 
the tarballs. All contents of this repository pertain to the work done for the
IPDPS 2017 conference.

The directory structure is as follows:

* [experiment name or number w.r.f. to the paper]
    * bin : all scripts+data required to run the experiment and generate raw 
            data.     
    * notebook : scripts/methods to analyze the raw data and generate plots
    * plots : all plots generated for internal understanding + all plots for 
            paper submission
    * raw_data : all raw data generated (only in the tarball)

The notebooks and plots (pdf, png) can be opened in github to view its 
contents.


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
