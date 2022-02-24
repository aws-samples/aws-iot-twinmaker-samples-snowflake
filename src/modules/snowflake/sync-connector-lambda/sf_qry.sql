select
  elem_name, coalesce(parent_name,'$ROOT') as parent_name,
  elem_parent_id, elem_id, attr_name, attr_pi_pt, comp_type
from
  my_table
