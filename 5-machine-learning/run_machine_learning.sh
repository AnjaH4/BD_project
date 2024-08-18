#!/bin/bash
#SBATCH --job-name="5-ML-daily-tickets"
#SBATCH --output=5-ML_%j.out
#SBATCH --error=5-ML_%j.err
#SBATCH --time=3:00:00 # job time limit - full format is D-H:M:S
#SBATCH --nodes=1 # number of nodes
#SBATCH --ntasks=1 # number of tasks, dodatki: constraint=h100
#SBATCH --partition=gpu 
#SBATCH --cpus-per-task=16 
#SBATCH --gres=gpu:1
#SBATCH --mem-per-gpu=32G 
#SBATCH --constraint=v100s

module load Anaconda3
source activate bigdata
srun python machine_learning.py
