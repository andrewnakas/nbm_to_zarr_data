"""Command-line interface for NBM to Zarr conversion."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from nbm_to_zarr.noaa.nbm_conus.forecast import NbmConusForecastDataset

app = typer.Typer(
    help="NBM to Zarr - NOAA National Blend of Models data reformatter",
    no_args_is_help=True,
)
console = Console()

# Dataset registry
DATASETS: dict[str, type] = {
    "noaa-nbm-conus-forecast": NbmConusForecastDataset,
}


@app.command()
def list_datasets() -> None:
    """List all available datasets."""
    table = Table(title="Available Datasets")
    table.add_column("ID", style="cyan")
    table.add_column("Description", style="green")

    for dataset_id, dataset_class in DATASETS.items():
        instance = dataset_class()
        attrs = instance.template_config.dataset_attributes
        table.add_row(dataset_id, attrs.description)

    console.print(table)


@app.command()
def update_template(
    dataset_id: Annotated[
        str,
        typer.Option(help="Dataset ID (use 'list-datasets' to see available options)"),
    ] = "noaa-nbm-conus-forecast",
    output_dir: Annotated[
        Path,
        typer.Option(help="Directory to save the template"),
    ] = Path("./templates"),
) -> None:
    """Generate and save a dataset template."""
    if dataset_id not in DATASETS:
        console.print(f"[red]Error: Unknown dataset ID '{dataset_id}'[/red]")
        console.print("Use 'list-datasets' to see available options")
        raise typer.Exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    dataset = DATASETS[dataset_id]()
    template_config = dataset.template_config

    # Generate template extending 7 days into the future
    import pandas as pd

    start_time = pd.Timestamp.now(tz="UTC").floor("H")
    template = template_config.get_template(
        append_dim_start=start_time,
        append_dim_periods=7 * 24,  # 7 days of hourly data
        append_dim_freq="1H",
    )

    # Save template
    template_path = template_config.template_path(output_dir)
    template.to_zarr(template_path, mode="w", consolidated=True)

    console.print(f"[green]Template saved to {template_path}[/green]")


@app.command()
def operational_update(
    dataset_id: Annotated[
        str,
        typer.Option(help="Dataset ID to update"),
    ] = "noaa-nbm-conus-forecast",
    output_dir: Annotated[
        Path,
        typer.Option(help="Directory to save the Zarr store"),
    ] = Path("./data"),
) -> None:
    """Run an operational update for a dataset."""
    import sys
    import traceback

    try:
        if dataset_id not in DATASETS:
            console.print(f"[red]Error: Unknown dataset ID '{dataset_id}'[/red]")
            console.print("Use 'list-datasets' to see available options")
            raise typer.Exit(1)

        output_dir.mkdir(parents=True, exist_ok=True)

        console.print(f"[cyan]Running operational update for {dataset_id}...[/cyan]")

        # Add detailed logging
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('/tmp/nbm_update.log')
            ]
        )
        logger = logging.getLogger(__name__)

        logger.info(f"Creating dataset instance for {dataset_id}")
        dataset = DATASETS[dataset_id]()

        logger.info(f"Starting operational update to {output_dir}")
        dataset.operational_update(output_dir)

        console.print("[green]Operational update complete![/green]")

    except KeyboardInterrupt:
        console.print("[yellow]Operation interrupted by user[/yellow]")
        sys.exit(130)
    except Exception as e:
        console.print(f"[red]FATAL ERROR: {e}[/red]")
        traceback.print_exc()
        console.print(f"\n[yellow]See /tmp/nbm_update.log for details[/yellow]")
        sys.exit(1)


@app.command()
def info(
    dataset_id: Annotated[
        str,
        typer.Argument(help="Dataset ID to show information for"),
    ] = "noaa-nbm-conus-forecast",
) -> None:
    """Show detailed information about a dataset."""
    if dataset_id not in DATASETS:
        console.print(f"[red]Error: Unknown dataset ID '{dataset_id}'[/red]")
        console.print("Use 'list-datasets' to see available options")
        raise typer.Exit(1)

    dataset = DATASETS[dataset_id]()
    attrs = dataset.template_config.dataset_attributes

    # Display dataset info
    console.print(f"\n[bold cyan]{attrs.title}[/bold cyan]")
    console.print(f"[dim]{attrs.description}[/dim]\n")

    table = Table(show_header=False, box=None)
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Dataset ID", attrs.id)
    table.add_row("Provider", attrs.provider)
    table.add_row("Model", attrs.model)
    table.add_row("Variant", attrs.variant)
    table.add_row("Version", attrs.version)

    console.print(table)

    # Display dimensions
    console.print("\n[bold]Dimensions:[/bold]")
    dim_table = Table(show_header=True)
    dim_table.add_column("Name", style="cyan")
    dim_table.add_column("Size", style="green")

    for dim, size in dataset.template_config.dimensions.items():
        dim_table.add_row(dim, str(size))

    console.print(dim_table)

    # Display variables
    console.print("\n[bold]Variables:[/bold]")
    var_table = Table(show_header=True)
    var_table.add_column("Name", style="cyan")
    var_table.add_column("Description", style="green")

    for var in dataset.template_config.data_vars:
        var_table.add_row(var.name, var.attrs.get("long_name", ""))

    console.print(var_table)


if __name__ == "__main__":
    app()
