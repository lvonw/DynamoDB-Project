FROM continuumio/miniconda3

# Set Workdir
WORKDIR /app

# Create conda environment
COPY environment.yml .
RUN conda env create -f environment.yml
SHELL ["conda", "run", "-n", "transfer_env", "/bin/bash", "-c"]

# Copy scripts
# Dockerfile has to be in the transferService directory because the COPY command unfortunately does not support moving up a folder
COPY /src/ /app/

# Wait for database to initialize and run script, keep container running after using 'tail'
#CMD ["bash", "-c", "source activate env && sleep 10 && python transferService.py && tail -f /dev/null"]
#CMD ["conda", "run", "-n", "env", "bash", "-c", "sleep 10 && python transferService.py && tail -f /dev/null"]
CMD ["bash", "-c", "sleep 10 && conda run -n env python transferService.py && tail -f /dev/null"]

