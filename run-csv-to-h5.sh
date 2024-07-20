#!/bin/bash
#SBATCH --job-name=csv-to-h5
#SBATCH --output=outputs/csv-to-h5.out
#SBATCH --error=errors/csv-to-h5.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --mem=32GB
#SBATCH --time=00:30:00

module load Anaconda3
source activate bigdata
python3 csv-to-h5.py