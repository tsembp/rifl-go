# Superseded first candidate run

This run measured a first version of the flat composite-key candidate. Its
acknowledgement GC re-seeked to the client's prefix after every delete. bbolt
keeps emptied leaves until commit, so every seek walked the emptied leaves
again, which is quadratic in the batch size. The median B=10,000 ACK was about
155 ms, against about 7 ms for the nested layout.

The 4-client and 1,024-client throughput results agree with the corrected run.
GC there deletes one record per request, so the defect has little effect. The
data is kept as a record of the defect. See `../composite2-20261111/REPORT.md`
for the corrected candidate.
