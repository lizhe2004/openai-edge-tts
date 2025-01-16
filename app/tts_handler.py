import edge_tts
import asyncio
import tempfile
import subprocess
import os
from pathlib import Path
import re
import json
from datetime import datetime
import logging
from mutagen.mp3 import MP3
# from mutagen.id3 import TIT2
from mutagen.easyid3 import EasyID3
import shutil
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Language default (environment variable)
DEFAULT_LANGUAGE = os.getenv('DEFAULT_LANGUAGE', 'en-US')

# Default speed from .env
DEFAULT_SPEED = float(os.getenv('DEFAULT_SPEED', '1.0'))

# Default output directory for saved files
DEFAULT_OUTPUT_DIR = os.getenv('TTS_OUTPUT_DIR', 'tts_output')

# Ensure the output directory exists
os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)

# Track temporary file names
TEMP_FILES = set()

# Function to load voice mappings from a JSON file
def load_voice_mappings(filepath='voice_mappings.json'):
    try:
        with open(filepath, 'r') as f:
            mappings = json.load(f)
            logging.info(f"Loaded voice mappings from {filepath}")
            return mappings
    except FileNotFoundError:
        logging.warning(f"{filepath} not found. Using default voice mappings.")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON in {filepath}: {e}. Using default voice mappings.")
        return {}

# Load voice mappings on startup
voice_mapping = load_voice_mappings()

def is_ffmpeg_installed():
    """Check if FFmpeg is installed and accessible."""
    try:
        subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def parse_voice_string(voice_string):
    """Parses the voice string to extract voice name, rate, and pitch adjustments."""
    match = re.match(r"([a-zA-Z]{2}-[A-Z]{2}-[a-zA-Z0-9]+)(?:([+-]\d+)[rR])?(?:([+-]\d+)[pP])?", voice_string)
    if not match:
        logging.warning(f"Invalid voice string format: {voice_string}")
        return voice_string, None, None

    base_voice = match.group(1)
    rate_str = match.group(2)
    pitch_str = match.group(3)

    rate_change = None
    pitch_change = None

    if rate_str:
        rate_change = int(rate_str)
        if not -99 <= rate_change <= 99:  # Basic rate validation
            logging.warning(f"Rate adjustment {rate_change} outside of reasonable bounds for: {voice_string}. Ignoring rate adjustment.")
            rate_change = None

    if pitch_str:
        pitch_change = int(pitch_str)
        if not -99 <= pitch_change <= 99:  # Basic pitch validation
            logging.warning(f"Pitch adjustment {pitch_change} outside of reasonable bounds for: {voice_string}. Ignoring pitch adjustment.")
            pitch_change = None

    logging.debug(f"Parsed voice string: {voice_string} -> base_voice: {base_voice}, rate_change: {rate_change}, pitch_change: {pitch_change}")
    return base_voice, rate_change, pitch_change

async def _delayed_cleanup(file_path, retries=3, delay=30):
    """Deletes a temporary file with retries."""
    for attempt in range(retries):
        try:
            await asyncio.sleep(delay)
            Path(file_path).unlink(missing_ok=True)
            TEMP_FILES.discard(file_path)  # Remove from tracking
            logging.debug(f"Deleted temporary file: {file_path} after {attempt+1} attempts")
            return
        except Exception as e:
            logging.error(f"Error deleting temp file: {file_path}, attempt {attempt+1}: {e}")
    logging.error(f"Failed to delete temp file: {file_path} after {retries} attempts.")

async def _save_audio_file(temp_file_path, text, edge_tts_voice, response_format, save_output=False, converted_file=False):
    """Saves the audio file, handles metadata, and cleans up temp files."""
    output_filename = None
    if not save_output:
        return temp_file_path
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if converted_file:
            output_filename = os.path.join(DEFAULT_OUTPUT_DIR, f"{edge_tts_voice.replace('-', '_')}_{timestamp}.{response_format}")
        else:
            output_filename = os.path.join(DEFAULT_OUTPUT_DIR, f"{edge_tts_voice.replace('-', '_')}_{timestamp}.mp3")

        # Copy the temp file and save it
        try:
            shutil.copy2(temp_file_path, output_filename)
            logging.debug(f"Copied temp file to: {output_filename}")
        except Exception as e:
            logging.error(f"Error copying temp file: {e}")
            return None

        # Add metadata to the copy
        if response_format == "mp3" and not converted_file:
            try:
                audio = MP3(output_filename, ID3=EasyID3)
                audio["title"] = text
                audio.save()
                logging.debug(f"Embedded text as title metadata in: {output_filename}")
            except Exception as e:
                logging.error(f"Error embedding metadata in {output_filename}: {e}")

        logging.info(f"Saved audio file to: {output_filename}")
        return output_filename
    except Exception as e:
        logging.error(f"Error saving audio file: {e}")
        return None
    finally:
         asyncio.create_task(_delayed_cleanup(temp_file_path))

async def _generate_audio(text, voice, response_format, default_speed):
    """Generate TTS audio with dynamic rate and pitch adjustments."""
    logging.info(f"Generating audio for text: '{text[:50]}...', voice: {voice}, format: {response_format}, default_speed: {default_speed}")

    save_output = False
    if voice.endswith('+s'):
        save_output = True
        voice = voice[:-2]  # Remove the '+s' flag
        logging.debug(f"Save output flag is set for voice: {voice}")

    # Check for voice mapping
    base_voice_name = voice_mapping.get(voice, voice)

    # Parse the voice string for adjustments
    edge_tts_voice, rate_change, pitch_change = parse_voice_string(base_voice_name)

    rate_value = speed_to_rate(default_speed)  # Use default speed if no rate in voice string
    pitch_value = "+0Hz" # default pitch when no pitch modifier
    if pitch_change is not None:
        pitch_value = f"{'+' if pitch_change >= 0 else ''}{pitch_change}Hz"

    if rate_change is not None:
        rate_value = f"{'+' if rate_change >= 0 else ''}{rate_change}%"

    temp_output_file = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3")
    TEMP_FILES.add(temp_output_file.name)


    try:
        communicator = edge_tts.Communicate(
            text=text,
            voice=edge_tts_voice,
            rate=rate_value,
            pitch=pitch_value
        )
        logging.debug(f"Edge-tts communicator initialized with voice: {edge_tts_voice}, rate: {rate_value}, pitch: {pitch_value}")
        await communicator.save(temp_output_file.name)
        logging.info(f"Successfully generated audio to temporary file: {temp_output_file.name}")

        if response_format == "mp3":
            if save_output:
                asyncio.create_task(_save_audio_file(temp_output_file.name, text, edge_tts_voice, response_format, save_output))
                return temp_output_file.name
            else:
                return temp_output_file.name

        if not is_ffmpeg_installed():
            logging.warning("FFmpeg is not available. Returning unmodified mp3 file.")
            return temp_output_file.name

        converted_output_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{response_format}")
        TEMP_FILES.add(converted_output_file.name)

        ffmpeg_command = [
            "ffmpeg",
            "-i", temp_output_file.name,
            "-c:a", {
                "aac": "aac",
                "mp3": "libmp3lame",
                "wav": "pcm_s16le",
                "opus": "libopus",
                "flac": "flac"
            }.get(response_format, "aac"),
            "-b:a", "192k" if response_format != "wav" else None,
            "-f", {
                "aac": "mp4",
                "mp3": "mp3",
                "wav": "wav",
                "opus": "ogg",
                "flac": "flac"
            }.get(response_format, response_format),
            "-y",
            converted_output_file.name
        ]

        try:
            logging.debug(f"Running FFmpeg command: {' '.join(ffmpeg_command)}")
            subprocess.run(ffmpeg_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logging.info(f"Successfully converted audio to: {response_format}")
        except subprocess.CalledProcessError as e:
            logging.error(f"FFmpeg error during audio conversion: {e.stderr.decode()}")
            raise RuntimeError(f"FFmpeg error during audio conversion: {e}")

        if save_output:
            asyncio.create_task(_save_audio_file(converted_output_file.name, text, edge_tts_voice, response_format, save_output, converted_file=True))
            return converted_output_file.name
        else:
            return converted_output_file.name

    except Exception as e:
        logging.error(f"Error during TTS generation: {e}")
        if temp_output_file and temp_output_file.name in TEMP_FILES:
            TEMP_FILES.discard(temp_output_file.name)
        raise

def generate_speech(text, voice, response_format, speed):
    try:
        return asyncio.run(_generate_audio(text, voice, response_format, speed))
    except Exception as e:
        logging.error(f"Error in generate_speech: {e}")
        return None

def get_models():
    return [
        {"id": "tts-1", "name": "Text-to-speech v1"},
        {"id": "tts-1-hd", "name": "Text-to-speech v1 HD"}
    ]

async def _get_voices(language=None):
    try:
        all_voices = await edge_tts.list_voices()
        language = language or DEFAULT_LANGUAGE
        filtered_voices = [
            {"name": v['ShortName'], "gender": v['Gender'], "language": v['Locale']}
            for v in all_voices if language == 'all' or language is None or v['Locale'] == language
        ]
        return filtered_voices
    except Exception as e:
        logging.error(f"Error retrieving voices from edge-tts: {e}")
        return []

def get_voices(language=None):
    return asyncio.run(_get_voices(language))

def speed_to_rate(speed: float) -> str:
    """
    Converts a multiplicative speed value to the edge-tts "rate" format.

    Args:
        speed (float): The multiplicative speed value (e.g., 1.5 for +50%, 0.5 for -50%).

    Returns:
        str: The formatted "rate" string (e.g., "+50%" or "-50%").
    """
    percentage_change = (speed - 1) * 100
    return f"{percentage_change:+.0f}%"

# Purge temp files on startup
for file_path in list(TEMP_FILES):
    try:
        Path(file_path).unlink(missing_ok=True)
        TEMP_FILES.discard(file_path)
        logging.info(f"Purged temp file on startup: {file_path}")
    except Exception as e:
        logging.error(f"Error purging temp file on startup: {file_path}: {e}")

"""
# Example usage (you would integrate this into your API endpoint logic)
if __name__ == "__main__":
    async def test_speech_generation():
        text = "This is a test of the new voice configuration, including metadata."

        # Test cases
        voices_to_test = [
            "en-US-AnaNeural",
            "en-US-AnaNeural-20r+10p",
            "en-US-AnaNeural+10r+10p",
            "en-US-AnaNeural-5p-13r",
            "en-US-AnaNeural-10p",
            "fable", # Assuming 'fable' is in your voice_mappings.json
            "alloy+s",
            "en-US-EmmaNeural+15r-5p+s"
        ]

        # Create a sample voice_mappings.json
        sample_mappings = {
            'fable': 'en-GB-SoniaNeural-5r+10p',
            'brave': 'en-US-BrandonNeural+20r-8p'
        }
        with open('voice_mappings.json', 'w') as f:
            json.dump(sample_mappings, f, indent=4)

        for voice in voices_to_test:
            print(f"Generating speech for voice: {voice}")
            output_file = generate_speech(text, voice, "mp3", DEFAULT_SPEED) # Pass DEFAULT_SPEED here for testing
            if output_file:
                print(f"Audio saved/generated at: {output_file}")
            else:
                print(f"Failed to generate audio for voice: {voice}")
            print("-" * 30)

    asyncio.run(test_speech_generation())
"""