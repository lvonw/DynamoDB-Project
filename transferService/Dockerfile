FROM continuumio/miniconda3

# Set Workdir
WORKDIR /app

# # Remove any existing environment for rebuilds
# SHELL ["/bin/bash", "-c"]
# RUN if [[ $(conda env list | grep 'transfer_env') ]] ; then \
#     source /opt/conda/etc/profile.d/conda.sh && \
#     conda activate base && \
#     conda env remove -n transfer_env -y || true ; \
#     fi

# # Create conda environment
# COPY environment.yml .
# RUN conda env create -f environment.yml
# SHELL ["conda", "run", "-n", "transfer_env", "/bin/bash", "-c"]

COPY requirements.txt .
SHELL ["/bin/bash", "-c"]
RUN source /opt/conda/etc/profile.d/conda.sh && \
    conda install -c conda-forge --file requirements.txt

# Copy scripts
# Dockerfile has to be in the transferService directory because the COPY command unfortunately does not support moving up a folder
COPY /src/ /app/

# Prevent python from buffering output so docker logs aren't buffered forever
#ENV PYTHONUNBUFFERED=1


#CMD ["bash", "-c", "source activate env && sleep 10 && python transferService.py && tail -f /dev/null"]
#CMD ["conda", "run", "-n", "env", "bash", "-c", "sleep 10 && python transferService.py && tail -f /dev/null"]

# Wait for database to initialize and run script, keep container running after
# using 'tail'. The -u flag ensures python outputs (and inputs) are unbuffered.
# CMD ["bash", \
#     "-c", \
#     "sleep 10 && conda run -n transfer_env python -u transferService.py && tail -f /dev/null"]
CMD ["bash", \
    "-c", \
    "sleep 12 && conda run python -u main.py && tail -f /dev/null"]