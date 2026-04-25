do $$
declare
  constraint_name text;
begin
  for constraint_name in
    select conname
    from pg_constraint
    where conrelid = 'public.stored_artifacts'::regclass
      and contype = 'c'
      and pg_get_constraintdef(oid) like '%storage_backend%'
  loop
    execute format(
      'alter table public.stored_artifacts drop constraint %I',
      constraint_name
    );
  end loop;
end $$;

alter table public.stored_artifacts
  add constraint stored_artifacts_storage_backend_check
  check (storage_backend in ('r2', 'supabase_storage'));
