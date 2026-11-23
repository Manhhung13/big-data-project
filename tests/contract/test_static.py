import ast
import json
from pathlib import Path
from stock_batch.config import repository_root


def test_schema_contracts_exist():
    root=repository_root()
    needed=['bronze_stock','bronze_news','stock_price','news','news_stock_mapping',
            'stock_features','news_density','stock_baseline','historical_events',
            'event_analysis','bronze_bad_records','quarantine']
    for name in needed:
        p=root/'contracts'/'schemas'/f'{name}.json'
        obj=json.loads(p.read_text(encoding='utf8'))
        assert obj['dataset']==name
        assert obj['schema_version']=='1.0'
        assert len(obj['fields'])>=5


def test_all_seven_jobs_have_transform_implementations():
    base=repository_root()/'src'/'stock_batch'/'transforms'
    for fn in ('stock.py','news.py','features.py','density.py','baseline.py','events.py','study.py'):
        tree=ast.parse((base/fn).read_text(encoding='utf8'))
        assert any(isinstance(n,ast.FunctionDef) for n in tree.body)


def test_required_adr_files_present():
    base=repository_root()/'docs'/'adr'
    for n in list(range(1,15))+[19]:
        assert (base/f'ADR-{n:02d}.md').exists()


def test_schema_contracts_match_schemas_py():
    """`python scripts/export_contracts.py` output must equal the committed contracts."""
    root=repository_root()
    tree=ast.parse((root/'src/stock_batch/schemas.py').read_text(encoding='utf8'))
    fields=next(ast.literal_eval(n.value) for n in ast.walk(tree)
                if isinstance(n,ast.Assign) and any(getattr(t,'id',None)=='fields' for t in n.targets))
    for name,cols in fields.items():
        doc=json.loads((root/'contracts'/'schemas'/f'{name}.json').read_text(encoding='utf8'))
        assert [(f['name'],f['type_code']) for f in doc['fields']]==[tuple(c) for c in cols], name


def test_every_configured_news_window_is_in_the_speed_row_contract():
    import yaml
    root=repository_root()
    enum=set(json.loads((root/'contracts/gold/speed_baseline_rows.schema.json').read_text(encoding='utf8'))
             ['properties']['window_size']['enum'])
    label=lambda m: '1h' if int(m)==60 else f'{int(m)}m'
    for env in ('dev','test','prod'):
        cfg=yaml.safe_load((root/'configs'/env/'config.yml').read_text(encoding='utf8'))
        windows={label(m) for m in cfg['news']['windows_minutes']} | {f"{int(cfg['market']['feature_minutes'])}m"}
        assert windows<=enum, (env, sorted(windows-enum))


def test_gold_contracts_are_valid_json_with_required_fields():
    root=repository_root()/'contracts'/'gold'
    pointer=json.loads((root/'speed_baseline.schema.json').read_text(encoding='utf8'))
    rows=json.loads((root/'speed_baseline_rows.schema.json').read_text(encoding='utf8'))
    assert {'baseline_version','train_end','row_count','status_counts','catalog_generation'}<=set(pointer['required'])
    from stock_batch.serving import SPEED_COLUMNS
    assert set(rows['required'])<=set(SPEED_COLUMNS) and set(SPEED_COLUMNS)<=set(rows['properties'])
