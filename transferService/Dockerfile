FROM continuumio/miniconda3

# Set Workdir
WORKDIR /app

# Add dependencies to conda environment that comes with this docker image
COPY requirements.txt .
SHELL ["/bin/bash", "-c"]
RUN source /opt/conda/etc/profile.d/conda.sh && \
    conda install -c conda-forge --file requirements.txt

# Copy scripts
# Dockerfile has to be in the transferService directory because the COPY command unfortunately does not support moving up a folder
COPY /src/ /app/

# Wait for database to initialize and run script, keep container running after
# using 'tail'. The -u flag ensures python outputs (and inputs) are unbuffered.
CMD ["bash", \
    "-c", \
    "sleep 12 && conda run python -u main.py && tail -f /dev/null"]