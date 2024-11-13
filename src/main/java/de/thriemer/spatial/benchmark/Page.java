package de.thriemer.spatial.benchmark;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
public class Page<T> {

    public int pageNumber;
    public int maxPageNumber;
    public List<T> data;

}
