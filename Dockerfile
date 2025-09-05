FROM golang:1.22
WORKDIR /
COPY storagegrid-operator-controller /storagegrid-operator-controller
USER 65532:65532
ENTRYPOINT ["/storagegrid-operator-controller"]
