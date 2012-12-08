queries = [
"""
SELECT grandparent_details.lfn
FROM wmbs_file_details
INNER JOIN wmbs_file_parent parent ON
wmbs_file_details.id = parent.child
INNER JOIN wmbs_file_parent grandparent ON
grandparent.child = parent.parent
INNER JOIN wmbs_file_details grandparent_details ON
grandparent.parent = grandparent_details.id
WHERE wmbs_file_details.lfn = '/store/data/Run2012D/Commissioning/RAW/v1/000/208/487/364F83FA-E93D-E211-9DA8-BCAEC53296F8.root'
""",
"""
SELECT COUNT(grandparent.parent)
FROM wmbs_file_details
INNER JOIN wmbs_file_parent parent ON
wmbs_file_details.id = parent.child
INNER JOIN wmbs_file_parent grandparent ON
grandparent.child = parent.parent
WHERE wmbs_file_details.lfn in ('/store/backfill/1/data/T0TEST_537p1_25ns_BUNNIES/Commissioning/RAW/v1/000/208/487/B271BF56-A140-E211-B75A-5404A63886EC.root',
'/store/backfill/1/data/T0TEST_537p1_25ns_BUNNIES/Commissioning/RAW/v1/000/208/487/AE0FAB62-A640-E211-8DAB-BCAEC5329702.root')
""",
"""
MERGE INTO lumi_section_closed lsc
USING(
SELECT lsc.run_id, lsc.stream_id, lsc.lumi_id, lsc.filecount, COUNT(streamer.streamer_id) AS whatWeHave
FROM lumi_section_closed lsc
INNER JOIN streamer ON
lsc.lumi_id = streamer.lumi_id
WHERE lsc.stream_id = 1 AND lsc.run_id = 208487 AND
lsc.lumi_id in (440,439,438,437,436,435,434,433,432,431,430,429,403,
402,400,399,398,397,393,390,389,388,387,385,384,383,382,380,
379,378,377,376,375,374,373,372,371,370,369,368,367,366,365,364,361,358,355,254,125)
GROUP BY lsc.run_id, lsc.stream_id, lsc.lumi_id, lsc.filecount
) other ON (other.run_id = lsc.run_id AND
other.stream_id = lsc.stream_id AND
other.lumi_id = lsc.lumi_id)
WHEN MATCHED THEN UPDATE
SET lsc.filecount = other.whatWeHave
""",
"""
SELECT DISTINCT lfn, events FROM wmbs_file_details
INNER JOIN wmbs_file_runlumi_map
ON wmbs_file_runlumi_map.fileid = wmbs_file_details.id
WHERE lfn like '/store/data/Run2012D/Commissioning/RAW/v1/000/208/487/%' AND 
lumi in (440,
439,
438,
437,
436,
435,
434,
433,
432,
431,
430,
429,
403,
402,
400,
399,
398,
397,
393,
390,
389,
388,
387,
385,
384,
383,
382,
380,
379,
378,
377,
376,
375,
374,
373,
372,
371,
370,
369,
368,
367,
366,
365,
364,
361,
358,
355,
254,
125)
"""
]