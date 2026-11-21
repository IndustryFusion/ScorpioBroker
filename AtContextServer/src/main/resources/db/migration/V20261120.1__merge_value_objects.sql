-- merge_has_value and merge_has_value_list merged every key of a value element by recursing into
-- merge_has_value, which expects an array. A JSON-LD keyword holds a scalar (@type, @language and @direction of a
-- value object, @id of a node reference) or an array of IRIs (@type of a node object), so a merge of
--   "value": {"@value": 80, "@type": "xsd:double"}   or   "value": {"@id": "urn:x"}
-- failed with 22023 "cannot get array length of a scalar", in merge entity and batch merge alike, even for a new
-- attribute.
-- Now a typed or language-tagged value object and a node reference are atomic: the new one replaces the old
-- element as a whole, so no mix of the old and the new one ({"@id": ..., "@value": ...}) can arise. Other
-- keywords of a node object are taken over as they are. A bare {"@value": x} is merged as before.
-- An element whose @value is urn:ngsi-ld:null is removed; its other keys are not applied to the element after it.
CREATE OR REPLACE FUNCTION public.merge_has_value(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	value jsonb;
	value2 jsonb;
	merged_json jsonb;
	index integer;
	removed integer;
	arr_idx integer;
	key text;
BEGIN
	if old_attrib is null then
		old_attrib := new_attrib;
	end if;
	if jsonb_array_length(new_attrib) != jsonb_array_length(old_attrib) then
		if new_attrib #>> '{0,@value}' = 'urn:ngsi-ld:null' then
			return null;
		else
			return new_attrib;
		end if;
	else
		index := 0;
		removed := 0;
		for value in select * from jsonb_array_elements(new_attrib) loop
			if (value ? '@value' and value ?| ARRAY['@type','@language','@direction'] and value ->> '@value' != 'urn:ngsi-ld:null')
					or (value ? '@id' and value - '@id' = '{}'::jsonb) then
				old_attrib = jsonb_set(old_attrib, ARRAY[index - removed]::text[], value);
				index := index + 1;
				continue;
			end if;
			for key, value2 in select * from jsonb_each(value) loop
				if key = '@value' then
					arr_idx := index - removed;
					if value2::text = '"urn:ngsi-ld:null"' then
						old_attrib = old_attrib - arr_idx;
						removed := removed + 1;
						exit;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY[arr_idx,key]::text[], value2);
					end if;
				elsif key like '@%' then
					arr_idx := index - removed;
					old_attrib = jsonb_set(old_attrib, ARRAY[arr_idx,key]::text[], value2);
				else
					arr_idx := index - removed;
					merged_json = merge_has_value(value2, old_attrib #> ARRAY[arr_idx,key]::text[]);
					if merged_json is null then
						old_attrib[arr_idx] = old_attrib[arr_idx] - key;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY[arr_idx,key]::text[], merged_json);
					end if;
				end if;
			end loop;
			index := index + 1;
		end loop;
		if jsonb_array_length(old_attrib) = 0 then
			return null;
		end if;
		return old_attrib;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_has_value_list(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	new_value_list jsonb;
	old_value_list jsonb;
	index integer;
	removed integer;
	value jsonb;
	key text;
	value2 jsonb;
	merged_json jsonb;
BEGIN
	new_value_list = new_attrib -> '@list';
	if old_attrib is null then
		old_attrib := new_attrib;
	end if;
	old_value_list = old_attrib -> '@list';
	if jsonb_array_length(new_value_list) != jsonb_array_length(old_value_list) then
		if new_value_list #>> '{0,@value}' = 'urn:ngsi-ld:null' then
			return null;
		else
			return new_attrib;
		end if;
	else
		index := 0;
		removed := 0;
		for value in select * from jsonb_array_elements(new_value_list) loop
			if (value ? '@value' and value ?| ARRAY['@type','@language','@direction'] and value ->> '@value' != 'urn:ngsi-ld:null')
					or (value ? '@id' and value - '@id' = '{}'::jsonb) then
				old_attrib = jsonb_set(old_attrib, ARRAY['@list',(index-removed)]::text[], value);
				index := index + 1;
				continue;
			end if;
			for key, value2 in select * from jsonb_each(value) loop
				if key = '@value' then
					if value2::text = '"urn:ngsi-ld:null"' then
						old_attrib = jsonb_set(old_attrib, ARRAY['@list']::text[], (old_attrib #> ARRAY['@list']::text[]) - (index-removed));
						removed := removed + 1;
						exit;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY['@list',(index-removed),key]::text[], value2);
					end if;
				elsif key = '@list' then
					merged_json = merge_has_value_list(value, old_value_list[index - removed]);
					if merged_json is null then
						old_attrib = jsonb_set(old_attrib, ARRAY['@list']::text[], (old_attrib #> ARRAY['@list']::text[]) - (index-removed));
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY['@list',(index-removed),key]::text[], merged_json);
					end if;
				elsif key like '@%' then
					old_attrib = jsonb_set(old_attrib, ARRAY['@list',(index-removed),key]::text[], value2);
				else
					merged_json = merge_has_value(value2, old_value_list[index - removed] -> key);
					if merged_json is null then
						old_attrib = jsonb_set(old_attrib, ARRAY['@list']::text[], (old_attrib #> ARRAY['@list']::text[]) - (index-removed));
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY['@list',(index-removed),key]::text[], merged_json);
					end if;
				end if;
			end loop;
			index := index + 1;
		end loop;
		if jsonb_array_length(old_value_list) = 0 then
			return null;
		end if;
		return old_attrib;
	end if;
END;
$BODY$;
