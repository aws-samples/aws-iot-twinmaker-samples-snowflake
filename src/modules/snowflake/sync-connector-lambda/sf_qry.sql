WITH vwNode(ELEM_ID, PARENT_ID) AS
(
SELECT ELEM_ID, ELEM_PARENT_ID FROM roci_invista.public.asset_mixer_alarm
)
,
cte AS
(
select c.elem_name as elem_name, coalesce(p.elem_name,'$ROOT') as parent_name,
c.elem_parent_id as elem_parent_id,
c.elem_id as elem_id, c.attr_name as attr_name, c.attr_pi_pt as attr_pi_pt, c.comp_type
 FROM vwNode n
 INNER JOIN roci_invista.public.asset_mixer_alarm c ON n.elem_id = c.elem_id
 INNER JOIN roci_invista.public.asset_mixer_alarm p ON n.parent_id = p.elem_id
)
,
entities AS
(
SELECT elem_name, parent_name, elem_id, elem_parent_id, comp_type,
array_agg(attr_name) attr_name,
array_agg(attr_pi_pt) attr_pi_pt
 FROM cte
 GROUP BY 1,2,3,4, 5
)

SELECT tr.epath, e.*
 FROM entities e
 INNER JOIN roci_invista.public.asset_mixer_alarm tr ON tr.Elem_id = e.elem_id
 ORDER BY tr.epath
