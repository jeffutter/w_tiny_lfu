defmodule WTinyLFU.BloomFilter do
  import Bitwise

  @doc """
  Create a new Bloom filter with the given capacity and false positive probability
  """
  @spec new(atom(), non_neg_integer(), float()) :: :ok
  def new(name, capacity, false_positive_probability) do
    factor = -:math.log(false_positive_probability) / (:math.log(2) * :math.log(2))
    bits = ceil(capacity * factor)
    hashes = ceil(:math.log(2) * bits / capacity)

    atomics = ceil(bits / 64)
    atomic_ref = :atomics.new(atomics, [{:signed, false}])

    :persistent_term.put({__MODULE__, name}, %{
      bits: bits,
      hashes: hashes,
      atomic_ref: atomic_ref
    })

    :ok
  end

  @doc """
  Insert a key into the filter
  """
  @spec insert(atom(), any()) :: :ok
  def insert(name, key) do
    %{bits: bits, hashes: hashes, atomic_ref: atomic_ref} = :persistent_term.get({__MODULE__, name})

    for i <- 1..hashes do
      pos = rem(Murmur.hash_x64_128(key, i), bits)
      atomic = floor(pos / 64) + 1
      bit = pos - (atomic - 1) * 64
      bitmask = 1 <<< bit

      atomic_insert(atomic_ref, atomic, bitmask)
    end

    :ok
  end

  @doc """
  Check if a key is included in the filter
  """
  @spec member?(atom(), any()) :: boolean()
  def member?(name, key) do
    %{bits: bits, hashes: hashes, atomic_ref: atomic_ref} = :persistent_term.get({__MODULE__, name})

    matches =
      for i <- 1..hashes do
        pos = rem(Murmur.hash_x64_128(key, i), bits)
        atomic = floor(pos / 64) + 1
        bit = pos - (atomic - 1) * 64
        bitmask = 1 <<< bit

        val = :atomics.get(atomic_ref, atomic)
        (val ||| bitmask) == val
      end

    Enum.all?(matches)
  end

  defp atomic_insert(ref, atomic, bitmask) do
    old_val = :atomics.get(ref, atomic)
    atomic_insert(ref, atomic, old_val, bitmask)
  end

  defp atomic_insert(ref, atomic, old_val, bitmask) do
    new_val = old_val ||| bitmask

    case :atomics.compare_exchange(ref, atomic, old_val, new_val) do
      :ok -> :ok
      i -> atomic_insert(ref, atomic, i, bitmask)
    end
  end

  @doc false
  @spec to_a(atom()) :: list(String.t())
  def to_a(name) do
    %{bits: bits, atomic_ref: atomic_ref} = :persistent_term.get({__MODULE__, name})

    atomics = ceil(bits / 64)

    for atomic <- 1..atomics do
      i = :atomics.get(atomic_ref, atomic)
      String.pad_leading(Integer.to_string(i, 2), 64, "0")
    end
  end
end
