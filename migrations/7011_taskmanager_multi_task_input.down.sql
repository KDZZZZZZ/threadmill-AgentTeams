ALTER TABLE taskmanager_contracts
  DROP CONSTRAINT IF EXISTS taskmanager_contracts_requirement_task_fkey;

WITH ranked AS (
  SELECT project_id, input_ref, task_id,
         row_number() OVER (PARTITION BY project_id, input_ref ORDER BY created_at, task_id) AS position
  FROM taskmanager_requirement_inputs
)
DELETE FROM taskmanager_requirement_inputs target
USING ranked
WHERE target.project_id = ranked.project_id
  AND target.input_ref = ranked.input_ref
  AND target.task_id = ranked.task_id
  AND ranked.position > 1;

ALTER TABLE taskmanager_requirement_inputs
  DROP CONSTRAINT IF EXISTS taskmanager_requirement_inputs_pkey;

ALTER TABLE taskmanager_requirement_inputs
  ADD PRIMARY KEY (project_id, input_ref);

ALTER TABLE taskmanager_contracts
  ADD CONSTRAINT taskmanager_contracts_project_id_input_ref_fkey
  FOREIGN KEY (project_id, input_ref)
  REFERENCES taskmanager_requirement_inputs(project_id, input_ref);
