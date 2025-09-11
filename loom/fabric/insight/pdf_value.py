
import numpy as np
from scipy import stats
from pydantic import BaseModel, Field


class PdfValue(BaseModel):
    """
    Represents the result of a Probability Density Function (PDF) analysis on a dataset.
    
    This class uses Kernel Density Estimation (KDE) to model the probability distribution
    of a set of data points. It identifies the most likely value (the peak of the PDF)
    and the range that contains the bulk of the probability mass, defined by the points
    at half the maximum density (similar to Full Width at Half Maximum).
    """
    left        : float = Field(description="The value on the left side of the peak where the density is half of the peak density (Full Width at Half Maximum left bound).")
    right       : float = Field(description="The value on the right side of the peak where the density is half of the peak density (Full Width at Half Maximum right bound).")
    peak        : float = Field(description="The value with the highest probability density (the peak of the PDF). Also known as the mode of the distribution.")
    density     : float = Field(description="The maximum density value at the peak.")
    jittered    : bool  = Field(description="True if a small amount of noise was added to the data to prevent a calculation error.", default=False)


    @classmethod
    def calculate(cls, data_points: np.ndarray) -> "PdfValue":
        """
        Calculates the PDF values from a given array of data points using Kernel
        Density Estimation (KDE).

        The process involves:
        1.  Handling edge cases: returns a default value for datasets with fewer than 2 points
            and filters out any NaN values.
        2.  Scaling the data to have a mean of 0 and a standard deviation of 1. This
            improves numerical stability during the KDE calculation.
        3.  Applying a gaussian KDE to the scaled data to create a smooth probability
            density function.
        4.  If the KDE calculation fails (e.g., due to degenerate data where all points
            are identical), a small amount of random noise ("jitter") is added to the
            data, and the calculation is retried.
        5.  Identifying the peak of the PDF, which corresponds to the most likely value (mode).
        6.  Finding the values to the left and right of the peak where the density is
            half of the peak's density, analogous to a Full Width at Half Maximum (FWHM).
        7.  Scaling the calculated peak, left, and right values back to the original
            data scale.

        Args:
            data_points: A numpy array of numerical data.

        Returns:
            An instance of PdfValue with the calculated metrics.
        """
        # A single data point is not enough for density estimation.
        if len(data_points) < 2:
            val = data_points[0] if len(data_points) > 0 else 0
            return PdfValue(left=val, right=val, peak=val, density=0)

        data_points_nan = np.isnan(data_points)
        if (data_points_nan.sum() > 0):
            print(f"data_points has NaN with {data_points_nan.sum()} nan out of {len(data_points_nan)}")
            data_points = data_points[~data_points_nan]

        # --- Scaling to prevent overflow ---
        mean = np.mean(data_points)
        std_dev = np.std(data_points)
        if std_dev > 0:
            scaled_data_points = (data_points - mean) / std_dev
        else:
            scaled_data_points = data_points # All points are the same

        added_jitter = False
        try:
            # Create a Kernel Density Estimation (KDE) of the data.
            # This gives us a smooth function representing the probability distribution.
            kde = stats.gaussian_kde(scaled_data_points)
            data = scaled_data_points
        except (np.linalg.LinAlgError, RuntimeError):
            # If the input data is degenerate (e.g., all points are the same),
            # gaussian_kde will fail. We add a tiny amount of random noise ("jitter")
            # to the data to make it non-degenerate and allow the calculation to proceed.
            jitter = np.random.normal(0, 1e-6, size=scaled_data_points.shape)
            data = scaled_data_points + jitter
            added_jitter = True
            try:
                kde = stats.gaussian_kde(data)
            except np.linalg.LinAlgError as e:
                # If it still fails, we cannot proceed and return a default error value.
                print(f"Returning error pdf value after jitter. gaussian_kde error: {e}")
                return PdfValue(left=0, right=0, peak=0, density=0, jittered=True)

        # Create a range of x-values to evaluate the PDF on.
        # A finer range will give more precise results.
        data_range = data.max() - data.min()
        # Use a padding that is 20% of the data range, or 1 if range is 0.
        padding = data_range * 0.2 if data_range > 0 else 1.0
        x_range = np.linspace(data.min() - padding, data.max() + padding, 500)

        # Calculate the PDF values for each x-value in the range.
        pdf_values = kde(x_range)

        # Find the peak of the PDF, which represents the most likely value.
        max_pdf_value = np.max(pdf_values)
        max_pdf_index = np.argmax(pdf_values)
        peak_x_scaled = x_range[max_pdf_index]

        # Calculate the value that is half of the peak's probability density.
        # This is used to find the flanking points.
        half_max_pdf_value = max_pdf_value / 2

        # --- Find the left flanking number ---
        left_side_indices = np.where(x_range < peak_x_scaled)[0]
        if len(left_side_indices) > 0:
            # Find the index on the left side where the PDF value is closest to the half-max value.
            left_half_max_index = (np.abs(pdf_values[left_side_indices] - half_max_pdf_value)).argmin()
            left_value_scaled = float(x_range[left_side_indices[left_half_max_index]])
        else:
            # If no points are to the left, use the start of the range.
            left_value_scaled = float(x_range[0])

        # --- Find the right flanking number ---
        right_side_indices = np.where(x_range > peak_x_scaled)[0]
        if len(right_side_indices) > 0:
            # Find the index on the right side where the PDF value is closest to the half-max value.
            right_half_max_index = (np.abs(pdf_values[right_side_indices] - half_max_pdf_value)).argmin()
            right_value_scaled = float(x_range[right_side_indices[right_half_max_index]])
        else:
            # If no points are to the right, use the end of the range.
            right_value_scaled = float(x_range[-1])

        # --- Inverse transform the scaled values ---
        if std_dev > 0:
            peak_x = peak_x_scaled * std_dev + mean
            left_value = left_value_scaled * std_dev + mean
            right_value = right_value_scaled * std_dev + mean
        else:
            peak_x = peak_x_scaled
            left_value = left_value_scaled
            right_value = right_value_scaled


        return cls(
            left=float(left_value),
            right=float(right_value),
            peak=peak_x,
            density=float(max_pdf_value),
            jittered=added_jitter
        )