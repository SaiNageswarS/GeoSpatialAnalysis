# Use Miniconda as base image
FROM continuumio/miniconda3

# Set working directory
WORKDIR /app

# Copy environment file
COPY environment.yml .

# Create environment and activate
RUN conda env create -f environment.yml

# Make sure to use bash shell for conda activate
SHELL ["conda", "run", "-n", "geospatial", "/bin/bash", "-c"]

# Copy the rest of the app
COPY . .

# Default command to run worker
CMD ["conda", "run", "-n", "geospatial", "python", "main.py"]
