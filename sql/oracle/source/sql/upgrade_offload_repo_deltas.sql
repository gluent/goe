/*
# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
*/

set termout off feedback off serveroutput on lines 200
spool sql/upgrade_offload_repo_deltas.tmp replace
declare

    v_current_version varchar2(30);

    type numbers_ntt is table of number;

    function to_numbers(v_version in varchar2) return numbers_ntt is
        v_numbers numbers_ntt := numbers_ntt();
    begin
        v_numbers.extend(2);
        v_numbers(1) := to_number(regexp_substr(v_version, '^[0-9]+'));
        v_numbers(2) := to_number(regexp_substr(v_version, '[0-9]+\.[0-9]+$'));
        return v_numbers;
    end to_numbers;

    function less_than(p_current_version in varchar2, p_target_version in varchar2) return boolean is
        v_current_version numbers_ntt;
        v_target_version numbers_ntt;
    begin
        v_current_version := to_numbers(p_current_version);
        v_target_version  := to_numbers(p_target_version);
        return (v_current_version(1) < v_target_version(1)) or (v_current_version(1) = v_target_version(1) and v_current_version(2) < v_target_version(2));
    end less_than;

    procedure check_version ( p_current_version in varchar2, p_target_version in varchar2 ) is
    begin
        if less_than(p_current_version, p_target_version) then
            dbms_output.put_line('@@create_offload_repo_' || replace(p_target_version, '.') || '.sql');
        end if;
    end check_version;

begin

    dbms_output.put_line(q'{-- Script generated by upgrade_offload_repo_deltas.sql}');
    dbms_output.put_line(q'{-- Copyright ' || to_char(sysdate, 'YYYY') || ' The GOE Authors. All rights reserved.}');
    dbms_output.put_line(q'{--}');

    select nvl(max(version), '0.0.0')
    into   v_current_version
    from   goe_version
    where  latest = 'Y';

    -- Follow this pattern for each repo version file in sequence...
    check_version(v_current_version, '1.0.0');

end;
/
spool off
set termout on feedback on serveroutput off

@@upgrade_offload_repo_deltas.tmp
