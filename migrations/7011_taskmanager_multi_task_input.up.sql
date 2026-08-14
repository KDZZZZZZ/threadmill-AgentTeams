ALTER TABLE taskmanager_contracts
  DROP CONSTRAINT IF EXISTS taskmanager_contracts_project_id_input_ref_fkey;

ALTER TABLE taskmanager_requirement_inputs
  DROP CONSTRAINT IF EXISTS taskmanager_requirement_inputs_pkey;

ALTER TABLE taskmanager_requirement_inputs
  ADD PRIMARY KEY (project_id, input_ref, task_id);

ALTER TABLE taskmanager_contracts
  ADD CONSTRAINT taskmanager_contracts_requirement_task_fkey
  FOREIGN KEY (project_id, input_ref, task_id)
  REFERENCES taskmanager_requirement_inputs(project_id, input_ref, task_id);
