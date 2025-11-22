import subprocess
from pathlib import Path
from rich.console import Console

console = Console()


def compile_report(
    report_path: str = "report.typ",
    output_path: str = "report.pdf",
) -> None:
    """
    Compiles a Typst report file to PDF.

    Args:
        report_path: Path to the Typst source file (.typ)
        output_path: Path where the PDF will be saved
    """
    report_file = Path(report_path)

    if not report_file.exists():
        raise FileNotFoundError(f"Report file not found: '{report_path}'")

    console.print(f"Compiling report from '{report_path}'")

    try:
        subprocess.run(
            ["typst", "compile", str(report_file), output_path],
            capture_output=True,
            text=True,
            check=True,
        )
        console.print(f"-> Report compiled successfully to '{output_path}'")
    except subprocess.CalledProcessError as e:
        console.print("[red]Error compiling report:[/red]")
        console.print(e.stderr)
        raise
    except FileNotFoundError:
        console.print(
            "[red]Error: 'typst' command not found. "
            "Please install Typst: https://typst.app/[/red]"
        )
        raise


def compile_slides(
    slides_path: str = "slides.typ",
    output_path: str = "slides.pdf",
) -> None:
    """
    Compiles a Typst slides file to PDF.

    Args:
        slides_path: Path to the Typst source file (.typ)
        output_path: Path where the PDF will be saved
    """
    slides_file = Path(slides_path)

    if not slides_file.exists():
        raise FileNotFoundError(f"Slides file not found: '{slides_path}'")

    console.print(f"Compiling slides from '{slides_path}'")

    try:
        subprocess.run(
            ["typst", "compile", str(slides_file), output_path],
            capture_output=True,
            text=True,
            check=True,
        )
        console.print(f"-> Slides compiled successfully to '{output_path}'")
    except subprocess.CalledProcessError as e:
        console.print("[red]Error compiling slides:[/red]")
        console.print(e.stderr)
        raise
    except FileNotFoundError:
        console.print(
            "[red]Error: 'typst' command not found. "
            "Please install Typst: https://typst.app/[/red]"
        )
        raise


def compile_all(
    report_path: str = "report.typ",
    slides_path: str = "slides.typ",
    report_output: str = "report.pdf",
    slides_output: str = "slides.pdf",
) -> None:
    """
    Compiles both report and slides Typst files to PDF.

    Args:
        report_path: Path to the report Typst source file (.typ)
        slides_path: Path to the slides Typst source file (.typ)
        report_output: Path where the report PDF will be saved
        slides_output: Path where the slides PDF will be saved
    """

    compile_report(report_path, report_output)
    compile_slides(slides_path, slides_output)

    console.print("-> All documents compiled successfully")
