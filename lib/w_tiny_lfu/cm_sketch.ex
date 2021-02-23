defmodule WTinyLFU.CMSketch do
  @doc """
  Create a new Count-Min Sketch of a given width and depth
  """
  @spec new(atom(), non_neg_integer(), non_neg_integer()) :: :ok
  def new(name, width, depth) do
    count = width * depth
    atomic_ref = :atomics.new(count, [{:signed, false}])

    :persistent_term.put({__MODULE__, name}, %{
      width: width,
      depth: depth,
      atomic_ref: atomic_ref
    })

    :ok
  end

  @doc """
  Insert the key into the sketch or increment it if it already exists
  """
  @spec insert(atom(), any()) :: non_neg_integer()
  def insert(name, key) do
    %{width: width, depth: depth, atomic_ref: atomic_ref} = :persistent_term.get({__MODULE__, name})

    counts =
      for seed <- 1..depth do
        slot = rem(Murmur.hash_x86_128(key, seed), width) + width * (seed - 1)
        :atomics.add_get(atomic_ref, slot, 1)
      end

    Enum.min(counts)
  end

  @doc """
  Get the count for a given key
  """
  @spec count(atom(), any()) :: non_neg_integer()
  def count(name, key) do
    %{width: width, depth: depth, atomic_ref: atomic_ref} = :persistent_term.get({__MODULE__, name})

    counts =
      for seed <- 1..depth do
        slot = rem(Murmur.hash_x86_128(key, seed), width) + width * (seed - 1)
        :atomics.get(atomic_ref, slot)
      end

    Enum.min(counts)
  end

  @doc false
  @spec to_a(atom()) :: list(list(non_neg_integer()))
  def to_a(name) do
    %{width: width, depth: depth, atomic_ref: atomic_ref} = :persistent_term.get({__MODULE__, name})

    for seed <- 1..depth do
      for slot <- 1..width do
        :atomics.get(atomic_ref, slot + width * (seed - 1))
      end
    end
  end
end
