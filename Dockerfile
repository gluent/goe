FROM gcr.io/cloud-devrel-public-resources/python-multi

WORKDIR /home/user

RUN pip install --no-cache-dir --upgrade pip nox

COPY . .

# In python-multi container the Python runtimes are already in PATH.
# No need to manipulate via ENV PATH.

ENV PYTHONUNBUFFERED=1

CMD [ "nox", "-s", "unit" ]
